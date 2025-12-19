package spcli

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/fatih/color"
	cbor "github.com/ipfs/go-ipld-cbor"
	"github.com/samber/lo"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-bitfield"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/builtin"
	miner2 "github.com/filecoin-project/specs-actors/v2/actors/builtin/miner"

	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/blockstore"
	"github.com/filecoin-project/lotus/chain/actors"
	"github.com/filecoin-project/lotus/chain/actors/adt"
	"github.com/filecoin-project/lotus/chain/actors/builtin/miner"
	"github.com/filecoin-project/lotus/chain/actors/builtin/verifreg"
	"github.com/filecoin-project/lotus/chain/actors/policy"
	"github.com/filecoin-project/lotus/chain/types"
	lcli "github.com/filecoin-project/lotus/cli"
	cliutil "github.com/filecoin-project/lotus/cli/util"
	"github.com/filecoin-project/lotus/lib/tablewriter"
)

type OnDiskInfoGetter func(cctx *cli.Context, id abi.SectorNumber, onChainInfo bool) (api.SectorInfo, error)

func SectorsStatusCmd(getActorAddress ActorAddressGetter, getOnDiskInfo OnDiskInfoGetter) *cli.Command {
	return &cli.Command{
		Name:      "status",
		Usage:     "Get the seal status of a sector by its number",
		ArgsUsage: "<sectorNum>",
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:    "log",
				Usage:   "display event log",
				Aliases: []string{"l"},
			},
			&cli.BoolFlag{
				Name:    "on-chain-info",
				Usage:   "show sector on chain info",
				Aliases: []string{"c"},
			},
			&cli.BoolFlag{
				Name:    "partition-info",
				Usage:   "show partition related info",
				Aliases: []string{"p"},
			},
			&cli.BoolFlag{
				Name:  "proof",
				Usage: "print snark proof bytes as hex",
			},
		},
		Action: func(cctx *cli.Context) error {
			ctx := lcli.ReqContext(cctx)

			if cctx.NArg() != 1 {
				return lcli.IncorrectNumArgs(cctx)
			}

			id, err := strconv.ParseUint(cctx.Args().First(), 10, 64)
			if err != nil {
				return err
			}

			onChainInfo := cctx.Bool("on-chain-info")

			var status api.SectorInfo
			if getOnDiskInfo != nil {
				status, err = getOnDiskInfo(cctx, abi.SectorNumber(id), onChainInfo)
				if err != nil {
					return err
				}
				fmt.Printf("SectorID:\t%d\n", status.SectorID)
				fmt.Printf("Status:\t\t%s\n", status.State)
				fmt.Printf("CIDcommD:\t%s\n", status.CommD)
				fmt.Printf("CIDcommR:\t%s\n", status.CommR)
				fmt.Printf("Ticket:\t\t%x\n", status.Ticket.Value)
				fmt.Printf("TicketH:\t%d\n", status.Ticket.Epoch)
				fmt.Printf("Seed:\t\t%x\n", status.Seed.Value)
				fmt.Printf("SeedH:\t\t%d\n", status.Seed.Epoch)
				fmt.Printf("Precommit:\t%s\n", status.PreCommitMsg)
				fmt.Printf("Commit:\t\t%s\n", status.CommitMsg)
				if cctx.Bool("proof") {
					fmt.Printf("Proof:\t\t%x\n", status.Proof)
				}
				fmt.Printf("Deals:\t\t%v\n", status.Deals)
				fmt.Printf("Retries:\t%d\n", status.Retries)
				if status.LastErr != "" {
					fmt.Printf("Last Error:\t\t%s\n", status.LastErr)
				}

				fmt.Printf("\nExpiration Info\n")
				fmt.Printf("OnTime:\t\t%v\n", status.OnTime)
				fmt.Printf("Early:\t\t%v\n", status.Early)

				var pamsHeaderOnce sync.Once

				for pi, piece := range status.Pieces {
					if piece.DealInfo == nil {
						continue
					}
					if piece.DealInfo.PieceActivationManifest == nil {
						continue
					}
					pamsHeaderOnce.Do(func() {
						fmt.Printf("\nPiece Activation Manifests\n")
					})

					pam := piece.DealInfo.PieceActivationManifest

					fmt.Printf("Piece %d: %s %s verif-alloc:%+v\n", pi, pam.CID, types.SizeStr(types.NewInt(uint64(pam.Size))), pam.VerifiedAllocationKey)
					for ni, notification := range pam.Notify {
						fmt.Printf("\tNotify %d: %s (%x)\n", ni, notification.Address, notification.Payload)
					}
				}

			} else {
				onChainInfo = true
			}

			maddr, err := getActorAddress(cctx)
			if err != nil {
				return err
			}

			if onChainInfo {
				fullApi, closer, err := lcli.GetFullNodeAPI(cctx)
				if err != nil {
					return err
				}
				defer closer()

				head, err := fullApi.ChainHead(ctx)
				if err != nil {
					return xerrors.Errorf("getting chain head: %w", err)
				}

				status, err := fullApi.StateSectorGetInfo(ctx, maddr, abi.SectorNumber(id), head.Key())
				if err != nil {
					return err
				}

				if status == nil {
					fmt.Println("Sector status not found on chain")
					return nil
				}

				mid, err := address.IDFromAddress(maddr)
				if err != nil {
					return err
				}
				fmt.Printf("\nSector On Chain Info\n")
				fmt.Printf("SealProof:\t\t%x\n", status.SealProof)
				fmt.Printf("Activation:\t\t%v\n", cliutil.EpochTime(head.Height(), status.Activation))
				fmt.Printf("Expiration:\t\t%s\n", cliutil.EpochTime(head.Height(), status.Expiration))
				fmt.Printf("DealWeight:\t\t%v\n", status.DealWeight)
				fmt.Printf("VerifiedDealWeight:\t\t%v\n", status.VerifiedDealWeight)
				fmt.Printf("InitialPledge:\t\t%v\n", types.FIL(status.InitialPledge))
				fmt.Printf("SectorID:\t\t{Miner: %v, Number: %v}\n", abi.ActorID(mid), status.SectorNumber)
			}

			if cctx.Bool("partition-info") {
				fullApi, nCloser, err := lcli.GetFullNodeAPI(cctx)
				if err != nil {
					return err
				}
				defer nCloser()

				maddr, err := getActorAddress(cctx)
				if err != nil {
					return err
				}

				mact, err := fullApi.StateGetActor(ctx, maddr, types.EmptyTSK)
				if err != nil {
					return err
				}

				tbs := blockstore.NewTieredBstore(blockstore.NewAPIBlockstore(fullApi), blockstore.NewMemory())
				mas, err := miner.Load(adt.WrapStore(ctx, cbor.NewCborStore(tbs)), mact)
				if err != nil {
					return err
				}

				errFound := errors.New("found")
				if err := mas.ForEachDeadline(func(dlIdx uint64, dl miner.Deadline) error {
					return dl.ForEachPartition(func(partIdx uint64, part miner.Partition) error {
						pas, err := part.AllSectors()
						if err != nil {
							return err
						}

						set, err := pas.IsSet(id)
						if err != nil {
							return err
						}
						if set {
							fmt.Printf("\nDeadline:\t%d\n", dlIdx)
							fmt.Printf("Partition:\t%d\n", partIdx)

							checkIn := func(name string, bg func() (bitfield.BitField, error)) error {
								bf, err := bg()
								if err != nil {
									return err
								}

								set, err := bf.IsSet(id)
								if err != nil {
									return err
								}
								setstr := "no"
								if set {
									setstr = "yes"
								}
								fmt.Printf("%s:   \t%s\n", name, setstr)
								return nil
							}

							if err := checkIn("Unproven", part.UnprovenSectors); err != nil {
								return err
							}
							if err := checkIn("Live", part.LiveSectors); err != nil {
								return err
							}
							if err := checkIn("Active", part.ActiveSectors); err != nil {
								return err
							}
							if err := checkIn("Faulty", part.FaultySectors); err != nil {
								return err
							}
							if err := checkIn("Recovering", part.RecoveringSectors); err != nil {
								return err
							}

							return errFound
						}

						return nil
					})
				}); err != errFound {
					if err != nil {
						return err
					}

					fmt.Println("\nNot found in any partition")
				}
			}

			if cctx.Bool("log") {
				if getOnDiskInfo != nil {
					fmt.Printf("--------\nEvent Log:\n")

					for i, l := range status.Log {
						fmt.Printf("%d.\t%s:\t[%s]\t%s\n", i, time.Unix(int64(l.Timestamp), 0), l.Kind, l.Message)
						if l.Trace != "" {
							fmt.Printf("\t%s\n", l.Trace)
						}
					}
				}
			}
			return nil
		},
	}
}

func SectorsListUpgradeBoundsCmd(getActorAddress ActorAddressGetter) *cli.Command {
	return &cli.Command{
		Name:  "upgrade-bounds",
		Usage: "Output upgrade bounds for available sectors",
		Flags: []cli.Flag{
			&cli.IntFlag{
				Name:  "buckets",
				Value: 25,
			},
			&cli.BoolFlag{
				Name:  "csv",
				Usage: "output machine-readable values",
			},
			&cli.BoolFlag{
				Name:  "deal-terms",
				Usage: "bucket by how many deal-sectors can start at a given expiration",
			},
		},
		Action: func(cctx *cli.Context) error {
			fullApi, closer2, err := lcli.GetFullNodeAPI(cctx)
			if err != nil {
				return err
			}
			defer closer2()

			ctx := lcli.ReqContext(cctx)

			head, err := fullApi.ChainHead(ctx)
			if err != nil {
				return xerrors.Errorf("getting chain head: %w", err)
			}

			maddr, err := getActorAddress(cctx)
			if err != nil {
				return err
			}

			list, err := fullApi.StateMinerActiveSectors(ctx, maddr, head.Key())
			if err != nil {
				return err
			}
			filter := bitfield.New()

			for _, s := range list {
				filter.Set(uint64(s.SectorNumber))
			}
			sset, err := fullApi.StateMinerSectors(ctx, maddr, &filter, head.Key())
			if err != nil {
				return err
			}

			if len(sset) == 0 {
				return nil
			}

			var minExpiration, maxExpiration abi.ChainEpoch

			for _, s := range sset {
				if s.Expiration < minExpiration || minExpiration == 0 {
					minExpiration = s.Expiration
				}
				if s.Expiration > maxExpiration {
					maxExpiration = s.Expiration
				}
			}

			buckets := cctx.Int("buckets")
			bucketSize := (maxExpiration - minExpiration) / abi.ChainEpoch(buckets)
			bucketCounts := make([]int, buckets+1)

			for b := range bucketCounts {
				bucketMin := minExpiration + abi.ChainEpoch(b)*bucketSize
				bucketMax := minExpiration + abi.ChainEpoch(b+1)*bucketSize

				if cctx.Bool("deal-terms") {
					bucketMax = bucketMax + policy.MarketDefaultAllocationTermBuffer
				}

				for _, s := range sset {
					isInBucket := s.Expiration >= bucketMin && s.Expiration < bucketMax

					if isInBucket {
						bucketCounts[b]++
					}
				}

			}

			// Creating CSV writer
			writer := csv.NewWriter(os.Stdout)

			// Writing CSV headers
			err = writer.Write([]string{"Max Expiration in Bucket", "Sector Count"})
			if err != nil {
				return xerrors.Errorf("writing csv headers: %w", err)
			}

			// Writing bucket details

			if cctx.Bool("csv") {
				for i := 0; i < buckets; i++ {
					maxExp := minExpiration + abi.ChainEpoch(i+1)*bucketSize

					timeStr := strconv.FormatInt(int64(maxExp), 10)

					err = writer.Write([]string{
						timeStr,
						strconv.Itoa(bucketCounts[i]),
					})
					if err != nil {
						return xerrors.Errorf("writing csv row: %w", err)
					}
				}

				// Flush to make sure all data is written to the underlying writer
				writer.Flush()

				if err := writer.Error(); err != nil {
					return xerrors.Errorf("flushing csv writer: %w", err)
				}

				return nil
			}

			tw := tablewriter.New(
				tablewriter.Col("Bucket Expiration"),
				tablewriter.Col("Sector Count"),
				tablewriter.Col("Bar"),
			)

			var barCols = 40
			var maxCount int

			for _, c := range bucketCounts {
				if c > maxCount {
					maxCount = c
				}
			}

			for i := 0; i < buckets; i++ {
				maxExp := minExpiration + abi.ChainEpoch(i+1)*bucketSize
				timeStr := cliutil.EpochTime(head.Height(), maxExp)

				tw.Write(map[string]interface{}{
					"Bucket Expiration": timeStr,
					"Sector Count":      color.YellowString("%d", bucketCounts[i]),
					"Bar":               "[" + color.GreenString(strings.Repeat("|", bucketCounts[i]*barCols/maxCount)) + strings.Repeat(" ", barCols-bucketCounts[i]*barCols/maxCount) + "]",
				})
			}

			return tw.Flush(os.Stdout)
		},
	}
}

func SectorPreCommitsCmd(getActorAddress ActorAddressGetter) *cli.Command {
	return &cli.Command{
		Name:  "precommits",
		Usage: "Print on-chain precommit info",
		Action: func(cctx *cli.Context) error {
			ctx := lcli.ReqContext(cctx)
			mapi, closer, err := lcli.GetFullNodeAPI(cctx)
			if err != nil {
				return err
			}
			defer closer()
			maddr, err := getActorAddress(cctx)
			if err != nil {
				return err
			}
			mact, err := mapi.StateGetActor(ctx, maddr, types.EmptyTSK)
			if err != nil {
				return err
			}
			store := adt.WrapStore(ctx, cbor.NewCborStore(blockstore.NewAPIBlockstore(mapi)))
			mst, err := miner.Load(store, mact)
			if err != nil {
				return err
			}
			preCommitSector := make([]miner.SectorPreCommitOnChainInfo, 0)
			err = mst.ForEachPrecommittedSector(func(info miner.SectorPreCommitOnChainInfo) error {
				preCommitSector = append(preCommitSector, info)
				return err
			})
			less := func(i, j int) bool {
				return preCommitSector[i].Info.SectorNumber <= preCommitSector[j].Info.SectorNumber
			}
			sort.Slice(preCommitSector, less)
			for _, info := range preCommitSector {
				fmt.Printf("%s: %s\n", info.Info.SectorNumber, info.PreCommitEpoch)
			}

			return nil
		},
	}
}

func SectorsCheckExpireCmd(getActorAddress ActorAddressGetter) *cli.Command {
	return &cli.Command{
		Name:  "check-expire",
		Usage: "Inspect expiring sectors",
		Flags: []cli.Flag{
			&cli.Int64Flag{
				Name:  "cutoff",
				Usage: "skip sectors whose current expiration is more than <cutoff> epochs from now, defaults to 60 days",
				Value: 172800,
			},
		},
		Action: func(cctx *cli.Context) error {

			fullApi, nCloser, err := lcli.GetFullNodeAPI(cctx)
			if err != nil {
				return err
			}
			defer nCloser()

			ctx := lcli.ReqContext(cctx)

			maddr, err := getActorAddress(cctx)
			if err != nil {
				return err
			}

			head, err := fullApi.ChainHead(ctx)
			if err != nil {
				return err
			}
			currEpoch := head.Height()

			nv, err := fullApi.StateNetworkVersion(ctx, types.EmptyTSK)
			if err != nil {
				return err
			}

			sectors, err := fullApi.StateMinerActiveSectors(ctx, maddr, types.EmptyTSK)
			if err != nil {
				return err
			}

			n := 0
			for _, s := range sectors {
				if s.Expiration-currEpoch <= abi.ChainEpoch(cctx.Int64("cutoff")) {
					sectors[n] = s
					n++
				}
			}
			sectors = sectors[:n]

			sort.Slice(sectors, func(i, j int) bool {
				if sectors[i].Expiration == sectors[j].Expiration {
					return sectors[i].SectorNumber < sectors[j].SectorNumber
				}
				return sectors[i].Expiration < sectors[j].Expiration
			})

			tw := tablewriter.New(
				tablewriter.Col("ID"),
				tablewriter.Col("SealProof"),
				tablewriter.Col("InitialPledge"),
				tablewriter.Col("Activation"),
				tablewriter.Col("Expiration"),
				tablewriter.Col("MaxExpiration"),
				tablewriter.Col("MaxExtendNow"))

			for _, sector := range sectors {
				MaxExpiration := sector.Activation + policy.GetSectorMaxLifetime(sector.SealProof, nv)
				maxExtension, err := policy.GetMaxSectorExpirationExtension(nv)
				if err != nil {
					return xerrors.Errorf("failed to get max extension: %w", err)
				}

				MaxExtendNow := currEpoch + maxExtension

				if MaxExtendNow > MaxExpiration {
					MaxExtendNow = MaxExpiration
				}

				tw.Write(map[string]interface{}{
					"ID":            sector.SectorNumber,
					"SealProof":     sector.SealProof,
					"InitialPledge": types.FIL(sector.InitialPledge).Short(),
					"Activation":    cliutil.EpochTime(currEpoch, sector.Activation),
					"Expiration":    cliutil.EpochTime(currEpoch, sector.Expiration),
					"MaxExpiration": cliutil.EpochTime(currEpoch, MaxExpiration),
					"MaxExtendNow":  cliutil.EpochTime(currEpoch, MaxExtendNow),
				})
			}

			return tw.Flush(os.Stdout)
		},
	}
}

func SectorsExtendCmd(getActorAddress ActorAddressGetter) *cli.Command {
	return &cli.Command{
		Name:      "extend",
		Usage:     "Extend expiring sectors while not exceeding each sector's max life",
		ArgsUsage: "<sectorNumbers...(optional)>",
		Flags: []cli.Flag{
			&cli.Int64Flag{
				Name:  "from",
				Usage: "only consider sectors whose current expiration epoch is in the range of [from, to], <from> defaults to: now + 120 (1 hour)",
			},
			&cli.Int64Flag{
				Name:  "to",
				Usage: "only consider sectors whose current expiration epoch is in the range of [from, to], <to> defaults to: now + 92160 (32 days)",
			},
			&cli.StringFlag{
				Name:  "sector-file",
				Usage: "provide a file containing one sector number in each line, ignoring above selecting criteria",
			},
			&cli.StringFlag{
				Name:  "exclude",
				Usage: "optionally provide a file containing excluding sectors",
			},
			&cli.Int64Flag{
				Name:  "extension",
				Usage: "try to extend selected sectors by this number of epochs, defaults to 540 days",
				Value: 1555200,
			},
			&cli.Int64Flag{
				Name:  "new-expiration",
				Usage: "try to extend selected sectors to this epoch, ignoring extension",
			},
			&cli.BoolFlag{
				Name:   "only-cc",
				Hidden: true,
			},
			&cli.BoolFlag{
				Name:  "drop-claims",
				Usage: "drop claims for sectors that can be extended, but only by dropping some of their verified power claims",
			},
			&cli.Int64Flag{
				Name:  "tolerance",
				Usage: "don't try to extend sectors by fewer than this number of epochs, defaults to 7 days",
				Value: 20160,
			},
			&cli.StringFlag{
				Name:  "max-fee",
				Usage: "use up to this amount of FIL for one message. pass this flag to avoid message congestion.",
				Value: "0",
			},
			&cli.Int64Flag{
				Name:  "max-sectors",
				Usage: "the maximum number of sectors contained in each message",
				Value: 500,
			},
			&cli.BoolFlag{
				Name:  "really-do-it",
				Usage: "pass this flag to really extend sectors, otherwise will only print out json representation of parameters",
			},
		},
		Action: func(cctx *cli.Context) error {
			if cctx.IsSet("only-cc") {
				return xerrors.Errorf("only-cc flag has been removed, use --exclude flag instead")
			}

			mf, err := types.ParseFIL(cctx.String("max-fee"))
			if err != nil {
				return err
			}

			spec := &api.MessageSendSpec{MaxFee: abi.TokenAmount(mf)}

			fullApi, nCloser, err := lcli.GetFullNodeAPI(cctx)
			if err != nil {
				return err
			}
			defer nCloser()

			ctx := lcli.ReqContext(cctx)

			maddr, err := getActorAddress(cctx)
			if err != nil {
				return err
			}

			head, err := fullApi.ChainHead(ctx)
			if err != nil {
				return err
			}
			currEpoch := head.Height()

			nv, err := fullApi.StateNetworkVersion(ctx, types.EmptyTSK)
			if err != nil {
				return err
			}

			activeSet, err := fullApi.StateMinerActiveSectors(ctx, maddr, types.EmptyTSK)
			if err != nil {
				return err
			}

			activeSectorsInfo := make(map[abi.SectorNumber]*miner.SectorOnChainInfo, len(activeSet))
			for _, info := range activeSet {
				activeSectorsInfo[info.SectorNumber] = info
			}

			mact, err := fullApi.StateGetActor(ctx, maddr, types.EmptyTSK)
			if err != nil {
				return err
			}

			tbs := blockstore.NewTieredBstore(blockstore.NewAPIBlockstore(fullApi), blockstore.NewMemory())
			adtStore := adt.WrapStore(ctx, cbor.NewCborStore(tbs))
			mas, err := miner.Load(adtStore, mact)
			if err != nil {
				return err
			}

			activeSectorsLocation := make(map[abi.SectorNumber]*miner.SectorLocation, len(activeSet))

			if err := mas.ForEachDeadline(func(dlIdx uint64, dl miner.Deadline) error {
				return dl.ForEachPartition(func(partIdx uint64, part miner.Partition) error {
					pas, err := part.ActiveSectors()
					if err != nil {
						return err
					}

					return pas.ForEach(func(i uint64) error {
						activeSectorsLocation[abi.SectorNumber(i)] = &miner.SectorLocation{
							Deadline:  dlIdx,
							Partition: partIdx,
						}
						return nil
					})
				})
			}); err != nil {
				return err
			}

			excludeSet := make(map[abi.SectorNumber]struct{})
			if cctx.IsSet("exclude") {
				excludeSectors, err := getSectorsFromFile(cctx.String("exclude"))
				if err != nil {
					return err
				}

				for _, id := range excludeSectors {
					excludeSet[id] = struct{}{}
				}
			}

			var sectors []abi.SectorNumber
			if cctx.Args().Present() {
				if cctx.IsSet("sector-file") {
					return xerrors.Errorf("sector-file specified along with command line params")
				}

				for i, s := range cctx.Args().Slice() {
					id, err := strconv.ParseUint(s, 10, 64)
					if err != nil {
						return xerrors.Errorf("could not parse sector %d: %w", i, err)
					}

					sectors = append(sectors, abi.SectorNumber(id))
				}
			} else if cctx.IsSet("sector-file") {
				sectors, err = getSectorsFromFile(cctx.String("sector-file"))
				if err != nil {
					return err
				}
			} else {
				from := currEpoch + 120
				to := currEpoch + 92160

				if cctx.IsSet("from") {
					from = abi.ChainEpoch(cctx.Int64("from"))
				}

				if cctx.IsSet("to") {
					to = abi.ChainEpoch(cctx.Int64("to"))
				}

				for _, si := range activeSet {
					if si.Expiration >= from && si.Expiration <= to {
						sectors = append(sectors, si.SectorNumber)
					}
				}
			}

			var sis []*miner.SectorOnChainInfo
			for _, id := range sectors {
				if _, exclude := excludeSet[id]; exclude {
					continue
				}

				si, found := activeSectorsInfo[id]
				if !found {
					return xerrors.Errorf("sector %d is not active", id)
				}

				sis = append(sis, si)
			}

			withinTolerance := func(a, b abi.ChainEpoch) bool {
				diff := a - b
				if diff < 0 {
					diff = -diff
				}

				return diff <= abi.ChainEpoch(cctx.Int64("tolerance"))
			}

			extensions := map[miner.SectorLocation]map[abi.ChainEpoch][]abi.SectorNumber{}
			for _, si := range sis {
				extension := abi.ChainEpoch(cctx.Int64("extension"))
				newExp := si.Expiration + extension

				if cctx.IsSet("new-expiration") {
					newExp = abi.ChainEpoch(cctx.Int64("new-expiration"))
				}

				maxExtension, err := policy.GetMaxSectorExpirationExtension(nv)
				if err != nil {
					return xerrors.Errorf("failed to get max extension: %w", err)
				}

				maxExtendNow := currEpoch + maxExtension
				if newExp > maxExtendNow {
					newExp = maxExtendNow
				}

				maxExp := si.Activation + policy.GetSectorMaxLifetime(si.SealProof, nv)
				if newExp > maxExp {
					newExp = maxExp
				}

				if newExp <= si.Expiration || withinTolerance(newExp, si.Expiration) {
					continue
				}

				l, found := activeSectorsLocation[si.SectorNumber]
				if !found {
					return xerrors.Errorf("location for sector %d not found", si.SectorNumber)
				}

				es, found := extensions[*l]
				if !found {
					ne := make(map[abi.ChainEpoch][]abi.SectorNumber)
					ne[newExp] = []abi.SectorNumber{si.SectorNumber}
					extensions[*l] = ne
				} else {
					added := false
					for exp := range es {
						if withinTolerance(newExp, exp) {
							es[exp] = append(es[exp], si.SectorNumber)
							added = true
							break
						}
					}

					if !added {
						es[newExp] = []abi.SectorNumber{si.SectorNumber}
					}
				}
			}

			verifregAct, err := fullApi.StateGetActor(ctx, builtin.VerifiedRegistryActorAddr, types.EmptyTSK)
			if err != nil {
				return xerrors.Errorf("failed to lookup verifreg actor: %w", err)
			}

			verifregSt, err := verifreg.Load(adtStore, verifregAct)
			if err != nil {
				return xerrors.Errorf("failed to load verifreg state: %w", err)
			}

			claimsMap, err := verifregSt.GetClaims(maddr)
			if err != nil {
				return xerrors.Errorf("failed to lookup claims for miner: %w", err)
			}

			claimIdsBySector, err := verifregSt.GetClaimIdsBySector(maddr)
			if err != nil {
				return xerrors.Errorf("failed to lookup claim IDs by sector: %w", err)
			}

			sectorsMax, err := policy.GetAddressedSectorsMax(nv)
			if err != nil {
				return err
			}

			addrSectors := sectorsMax
			if cctx.Int("max-sectors") != 0 {
				addrSectors = cctx.Int("max-sectors")
				if addrSectors > sectorsMax {
					return xerrors.Errorf("the specified max-sectors exceeds the maximum limit")
				}
			}

			var params []miner.ExtendSectorExpiration2Params

			for l, exts := range extensions {
				for newExp, numbers := range exts {
					batchSize := addrSectors

					// The unfortunate thing about this approach is that batches less than batchSize in different partitions cannot be aggregated together to send messages.
					for i := 0; i < len(numbers); i += batchSize {
						end := i + batchSize
						if end > len(numbers) {
							end = len(numbers)
						}

						batch := numbers[i:end]

						sectorsWithoutClaimsToExtend := bitfield.New()
						numbersToExtend := make([]abi.SectorNumber, 0, len(numbers))
						var sectorsWithClaims []miner.SectorClaim
						p := miner.ExtendSectorExpiration2Params{}

						for _, sectorNumber := range batch {
							claimIdsToMaintain := make([]verifreg.ClaimId, 0)
							claimIdsToDrop := make([]verifreg.ClaimId, 0)
							cannotExtendSector := false
							claimIds, ok := claimIdsBySector[sectorNumber]
							// Nothing to check, add to ccSectors
							if !ok {
								sectorsWithoutClaimsToExtend.Set(uint64(sectorNumber))
								numbersToExtend = append(numbersToExtend, sectorNumber)
							} else {
								for _, claimId := range claimIds {
									claim, ok := claimsMap[claimId]
									if !ok {
										return xerrors.Errorf("failed to find claim for claimId %d", claimId)
									}
									claimExpiration := claim.TermStart + claim.TermMax
									// can be maintained in the extended sector
									if claimExpiration > newExp {
										claimIdsToMaintain = append(claimIdsToMaintain, claimId)
									} else {
										sectorInfo, ok := activeSectorsInfo[sectorNumber]
										if !ok {
											return xerrors.Errorf("failed to find sector in active sector set: %w", err)
										}
										if !cctx.Bool("drop-claims") {
											fmt.Printf("skipping sector %d because claim %d (client f0%s, piece %s) cannot be maintained in the extended sector (use --drop-claims to drop claims)\n", sectorNumber, claimId, claim.Client, claim.Data)
											cannotExtendSector = true
											break
										} else if currEpoch <= (claim.TermStart + claim.TermMin) {
											// FIP-0045 requires the claim minimum duration to have passed
											fmt.Printf("skipping sector %d because claim %d (client f0%s, piece %s) has not reached its minimum duration\n", sectorNumber, claimId, claim.Client, claim.Data)
											cannotExtendSector = true
											break
										} else if currEpoch <= sectorInfo.Expiration-builtin.EndOfLifeClaimDropPeriod {
											// FIP-0045 requires the sector to be in its last 30 days of life
											fmt.Printf("skipping sector %d because claim %d (client f0%s, piece %s) is not in its last 30 days of life\n", sectorNumber, claimId, claim.Client, claim.Data)
											cannotExtendSector = true
											break
										}

										claimIdsToDrop = append(claimIdsToDrop, claimId)
									}

									numbersToExtend = append(numbersToExtend, sectorNumber)
								}
								if cannotExtendSector {
									continue
								}

								if len(claimIdsToMaintain)+len(claimIdsToDrop) != 0 {
									sectorsWithClaims = append(sectorsWithClaims, miner.SectorClaim{
										SectorNumber:   sectorNumber,
										MaintainClaims: claimIdsToMaintain,
										DropClaims:     claimIdsToDrop,
									})
								}
							}
						}

						p.Extensions = append(p.Extensions, miner.ExpirationExtension2{
							Deadline:          l.Deadline,
							Partition:         l.Partition,
							Sectors:           SectorNumsToBitfield(numbersToExtend),
							SectorsWithClaims: sectorsWithClaims,
							NewExpiration:     newExp,
						})
						params = append(params, p)
					}

				}
			}

			if len(params) == 0 {
				fmt.Println("nothing to extend")
				return nil
			}

			mi, err := fullApi.StateMinerInfo(ctx, maddr, types.EmptyTSK)
			if err != nil {
				return xerrors.Errorf("getting miner info: %w", err)
			}

			stotal := 0

			for i := range params {
				scount := 0
				for _, ext := range params[i].Extensions {
					count, err := ext.Sectors.Count()
					if err != nil {
						return err
					}
					scount += int(count)
				}
				fmt.Printf("Extending %d sectors: ", scount)
				stotal += scount

				sp, aerr := actors.SerializeParams(&params[i])
				if aerr != nil {
					return xerrors.Errorf("serializing params: %w", err)
				}

				m := &types.Message{
					From:   mi.Worker,
					To:     maddr,
					Method: builtin.MethodsMiner.ExtendSectorExpiration2,
					Value:  big.Zero(),
					Params: sp,
				}

				if !cctx.Bool("really-do-it") {
					pp, err := NewPseudoExtendParams(&params[i])
					if err != nil {
						return err
					}

					data, err := json.MarshalIndent(pp, "", "  ")
					if err != nil {
						return err
					}

					fmt.Println("\n", string(data))

					_, err = fullApi.GasEstimateMessageGas(ctx, m, spec, types.EmptyTSK)
					if err != nil {
						return xerrors.Errorf("simulating message execution: %w", err)
					}

					continue
				}

				smsg, err := fullApi.MpoolPushMessage(ctx, m, spec)
				if err != nil {
					return xerrors.Errorf("mpool push message: %w", err)
				}

				fmt.Println(smsg.Cid())
			}

			fmt.Printf("%d sectors extended\n", stotal)

			return nil
		},
	}
}

func SectorNumsToBitfield(sectors []abi.SectorNumber) bitfield.BitField {
	var numbers []uint64
	for _, sector := range sectors {
		numbers = append(numbers, uint64(sector))
	}

	return bitfield.NewFromSet(numbers)
}

func getSectorsFromFile(filePath string) ([]abi.SectorNumber, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}

	scanner := bufio.NewScanner(file)
	sectors := make([]abi.SectorNumber, 0)

	for scanner.Scan() {
		line := scanner.Text()

		id, err := strconv.ParseUint(line, 10, 64)
		if err != nil {
			return nil, xerrors.Errorf("could not parse %s as sector id: %s", line, err)
		}

		sectors = append(sectors, abi.SectorNumber(id))
	}

	if err = file.Close(); err != nil {
		return nil, err
	}

	return sectors, nil
}

func NewPseudoExtendParams(p *miner.ExtendSectorExpiration2Params) (*PseudoExtendSectorExpirationParams, error) {
	res := PseudoExtendSectorExpirationParams{}
	for _, ext := range p.Extensions {
		scount, err := ext.Sectors.Count()
		if err != nil {
			return nil, err
		}

		sectors, err := ext.Sectors.All(scount)
		if err != nil {
			return nil, err
		}

		res.Extensions = append(res.Extensions, PseudoExpirationExtension{
			Deadline:      ext.Deadline,
			Partition:     ext.Partition,
			Sectors:       ArrayToString(sectors),
			NewExpiration: ext.NewExpiration,
		})
	}
	return &res, nil
}

type PseudoExtendSectorExpirationParams struct {
	Extensions []PseudoExpirationExtension
}

type PseudoExpirationExtension struct {
	Deadline      uint64
	Partition     uint64
	Sectors       string
	NewExpiration abi.ChainEpoch
}

// ArrayToString Example: {1,3,4,5,8,9} -> "1,3-5,8-9"
func ArrayToString(array []uint64) string {
	sort.Slice(array, func(i, j int) bool {
		return array[i] < array[j]
	})

	var sarray []string
	s := ""

	for i, elm := range array {
		if i == 0 {
			s = strconv.FormatUint(elm, 10)
			continue
		}
		if elm == array[i-1] {
			continue // filter out duplicates
		} else if elm == array[i-1]+1 {
			s = strings.Split(s, "-")[0] + "-" + strconv.FormatUint(elm, 10)
		} else {
			sarray = append(sarray, s)
			s = strconv.FormatUint(elm, 10)
		}
	}

	if s != "" {
		sarray = append(sarray, s)
	}

	return strings.Join(sarray, ",")
}

func SectorsCompactPartitionsCmd(getActorAddress ActorAddressGetter) *cli.Command {
	return &cli.Command{
		Name:  "compact-partitions",
		Usage: "removes dead sectors from partitions and reduces the number of partitions used if possible",
		Flags: []cli.Flag{
			&cli.Uint64Flag{
				Name:     "deadline",
				Usage:    "the deadline to compact the partitions in",
				Required: true,
			},
			&cli.Int64SliceFlag{
				Name:     "partitions",
				Usage:    "list of partitions to compact sectors in",
				Required: true,
			},
			&cli.BoolFlag{
				Name:  "really-do-it",
				Usage: "Actually send transaction performing the action",
				Value: false,
			},
		},
		Action: func(cctx *cli.Context) error {
			fullNodeAPI, acloser, err := lcli.GetFullNodeAPI(cctx)
			if err != nil {
				return err
			}
			defer acloser()

			ctx := lcli.ReqContext(cctx)

			maddr, err := getActorAddress(cctx)
			if err != nil {
				return err
			}

			minfo, err := fullNodeAPI.StateMinerInfo(ctx, maddr, types.EmptyTSK)
			if err != nil {
				return err
			}

			deadline := cctx.Uint64("deadline")
			if deadline > miner.WPoStPeriodDeadlines {
				return fmt.Errorf("deadline %d out of range", deadline)
			}

			parts := cctx.Int64Slice("partitions")
			if len(parts) <= 0 {
				return fmt.Errorf("must include at least one partition to compact")
			}
			fmt.Printf("compacting %d partitions\n", len(parts))

			var makeMsgForPartitions func(partitionsBf bitfield.BitField) ([]*types.Message, error)
			makeMsgForPartitions = func(partitionsBf bitfield.BitField) ([]*types.Message, error) {
				params := miner.CompactPartitionsParams{
					Deadline:   deadline,
					Partitions: partitionsBf,
				}

				sp, aerr := actors.SerializeParams(&params)
				if aerr != nil {
					return nil, xerrors.Errorf("serializing params: %w", err)
				}

				msg := &types.Message{
					From:   minfo.Worker,
					To:     maddr,
					Method: builtin.MethodsMiner.CompactPartitions,
					Value:  big.Zero(),
					Params: sp,
				}

				estimatedMsg, err := fullNodeAPI.GasEstimateMessageGas(ctx, msg, nil, types.EmptyTSK)
				if err != nil && errors.Is(err, &api.ErrOutOfGas{}) {
					// the message is too big -- split into 2
					partitionsSlice, err := partitionsBf.All(math.MaxUint64)
					if err != nil {
						return nil, err
					}

					partitions1 := bitfield.New()
					for i := 0; i < len(partitionsSlice)/2; i++ {
						partitions1.Set(uint64(i))
					}

					msgs1, err := makeMsgForPartitions(partitions1)
					if err != nil {
						return nil, err
					}

					// time for the second half
					partitions2 := bitfield.New()
					for i := len(partitionsSlice) / 2; i < len(partitionsSlice); i++ {
						partitions2.Set(uint64(i))
					}

					msgs2, err := makeMsgForPartitions(partitions2)
					if err != nil {
						return nil, err
					}

					return append(msgs1, msgs2...), nil
				} else if err != nil {
					return nil, err
				}

				return []*types.Message{estimatedMsg}, nil
			}

			partitions := bitfield.New()
			for _, partition := range parts {
				partitions.Set(uint64(partition))
			}

			msgs, err := makeMsgForPartitions(partitions)
			if err != nil {
				return xerrors.Errorf("failed to make messages: %w", err)
			}

			// Actually send the messages if really-do-it provided, simulate otherwise
			if cctx.Bool("really-do-it") {
				smsgs, err := fullNodeAPI.MpoolBatchPushMessage(ctx, msgs, nil)
				if err != nil {
					return xerrors.Errorf("mpool push: %w", err)
				}

				if len(smsgs) == 1 {
					fmt.Printf("Requested compact partitions in message %s\n", smsgs[0].Cid())
				} else {
					fmt.Printf("Requested compact partitions in %d messages\n\n", len(smsgs))
					for _, v := range smsgs {
						fmt.Println(v.Cid())
					}
				}

				for _, v := range smsgs {
					wait, err := fullNodeAPI.StateWaitMsg(ctx, v.Cid(), 2)
					if err != nil {
						return err
					}

					// check it executed successfully
					if wait.Receipt.ExitCode.IsError() {
						fmt.Println(cctx.App.Writer, "compact partitions msg %s failed!", v.Cid())
						return err
					}
				}

				return nil
			}

			for i, v := range msgs {
				fmt.Printf("total of %d CompactPartitions msgs would be sent\n", len(msgs))

				estMsg, err := fullNodeAPI.GasEstimateMessageGas(ctx, v, nil, types.EmptyTSK)
				if err != nil {
					return err
				}

				fmt.Printf("msg %d would cost up to %s\n", i+1, types.FIL(estMsg.RequiredFunds()))
			}

			return nil

		},
	}
}

var tsns = []int{
	115404,
	115412,
	115413,
	115418,
	115419,
	115420,
	115421,
	115422,
	115423,
	115426,
	115427,
	115428,
	115429,
	115430,
	115431,
	115432,
	115433,
	115434,
	115435,
	115436,
	115437,
	115438,
	115439,
	115440,
	115441,
	115442,
	115443,
	115444,
	115445,
	115446,
	115448,
	115449,
	116153,
	116154,
	116155,
	116158,
	116162,
	116163,
	116164,
	116175,
	116176,
	116177,
	116178,
	116180,
	116181,
	116182,
	116183,
	116184,
	116185,
	116186,
	116187,
	116188,
	116190,
	116191,
	116193,
	116194,
	116195,
	116196,
	116200,
	116201,
	116202,
	116203,
	116204,
	116205,
	116209,
	116210,
	116765,
	116766,
	116786,
	116796,
	116810,
	116811,
	116813,
	116819,
	116820,
	116821,
	116822,
	116823,
	116824,
	116826,
	116827,
	116828,
	116833,
	116834,
	116835,
	116836,
	116837,
	116841,
	116846,
	116852,
	116854,
	117425,
	117426,
	117427,
	117428,
	117439,
	117449,
	117453,
	117455,
	117464,
	117465,
	117466,
	117468,
	117469,
	117470,
	117472,
	117474,
	117475,
	117476,
	117478,
	117479,
	117480,
	117481,
	117482,
	117485,
	117486,
	117665,
	117686,
	117691,
	117692,
	117693,
	117694,
	117695,
	117696,
	117698,
	117700,
	117701,
	117702,
	117703,
	117704,
	117705,
	117706,
	117707,
	117708,
	117709,
	117710,
	117712,
	117713,
	117714,
	117715,
	117716,
	117717,
	117718,
	117719,
	117720,
	117724,
	117725,
	117726,
	117727,
	117728,
	117729,
	117730,
	117731,
	117732,
	117733,
	117734,
	117735,
	117736,
	117737,
	117738,
	117739,
	117740,
	117741,
	117742,
	117743,
	117744,
	117745,
	117746,
	117747,
	117748,
	117749,
	117750,
	117751,
	117752,
	117753,
	117754,
	117755,
	117756,
	117757,
	117758,
	117759,
	117760,
	117761,
	117762,
	117763,
	117764,
	117765,
	117766,
	117767,
	117768,
	117769,
	117770,
	117771,
	117772,
	117773,
	117774,
	117775,
	117776,
	117777,
	117778,
	117779,
	117780,
	117781,
	117782,
	117783,
	117784,
	117785,
	117786,
	117787,
	117788,
	117789,
	117790,
	117791,
	117792,
	117793,
	117794,
	117795,
	117796,
	117797,
	117798,
	117799,
	117800,
	117801,
	117802,
	117803,
	117804,
	117805,
	117806,
	117807,
	117808,
	117809,
	117810,
	117811,
	117812,
	117813,
	117814,
	117815,
	117816,
	117817,
	117818,
	117819,
	117820,
	117821,
	117822,
	117823,
	117824,
	117825,
	117826,
	117827,
	117828,
	117829,
	117830,
	117831,
	117832,
	117833,
	117834,
	117835,
	117836,
	117837,
	117838,
	117839,
	117840,
	117841,
	117842,
	117843,
	117844,
	117845,
	117846,
	117847,
	117848,
	117849,
	117850,
	117851,
	117852,
	117853,
	117854,
	117855,
	117856,
	117857,
	117858,
	117859,
	117860,
	117861,
	117862,
	117863,
	117864,
	117865,
	117866,
	117867,
	117868,
	117869,
	117870,
	117871,
	117872,
	117873,
	117874,
	117875,
	117876,
	117877,
	117878,
	117879,
	117880,
	117881,
	117882,
	117883,
	117884,
	117885,
	117886,
	117887,
	117888,
	117889,
	117890,
	117891,
	117892,
	117893,
	117894,
	117895,
	117896,
	117897,
	117898,
	117899,
	117900,
	117901,
	117902,
	117903,
	117904,
	117905,
	117906,
	117907,
	117908,
	117909,
	117910,
	117911,
	117912,
	117913,
	117914,
	117915,
	117916,
	117917,
	117918,
	117919,
	117920,
	117921,
	117922,
	117923,
	117924,
	117925,
	117926,
	117927,
	117928,
	117929,
	117930,
	117931,
	117932,
	117933,
	117934,
	117935,
	117936,
	117937,
	117938,
	117939,
	117940,
	117941,
	117942,
	117943,
	117944,
	117945,
	117946,
	117947,
	117948,
	117949,
	117950,
	117951,
	117952,
	117953,
	117954,
	117955,
	117956,
	117957,
	117958,
	117959,
	117960,
	117961,
	117962,
	117963,
	117964,
	117965,
	117966,
	117967,
	117968,
	117969,
	117970,
	117971,
	117972,
	117973,
	117974,
	117975,
	117976,
	117977,
	117978,
	117979,
	117980,
	117981,
	117982,
	117983,
	117984,
	117985,
	117986,
	117987,
	117988,
	117989,
	117990,
	117991,
	117992,
	117993,
	117994,
	117995,
	117996,
	117997,
	117998,
	117999,
	118000,
	118001,
	118002,
	118003,
	118004,
	118005,
	118006,
	118007,
	118008,
	118009,
	118010,
	118011,
	118012,
	118013,
	118014,
	118015,
	118016,
	118017,
	118018,
	118019,
	118020,
	118021,
	118022,
	118023,
	118024,
	118025,
	118026,
	118027,
	118028,
	118029,
	118030,
	118031,
	118032,
	118033,
	118034,
	118035,
	118036,
	118037,
	118038,
	118039,
	118040,
	118041,
	118042,
	118043,
	118044,
	118045,
	118046,
	118047,
	118048,
	118049,
	118050,
	118051,
	118052,
	118053,
	118054,
	118055,
	118056,
	118057,
	118058,
	118059,
	118060,
	118061,
	118062,
	118063,
	118064,
	118065,
	118066,
	118067,
	118068,
	118069,
	118070,
	118071,
	118072,
	118073,
	118074,
	118075,
	118076,
	118077,
	118078,
	118079,
	118080,
	118081,
	118082,
	118083,
	118084,
	118085,
	118086,
	118087,
	118088,
	118089,
	118090,
	118091,
	118092,
	118093,
	118094,
	118095,
	118096,
	118097,
	118098,
	118099,
	118100,
	118101,
	118102,
	118103,
	118104,
	118105,
	118106,
	118107,
	118108,
	118109,
	118110,
	118111,
	118112,
	118113,
	118114,
	118115,
	118116,
	118117,
	118118,
	118119,
	118120,
	118121,
	118122,
	118123,
	118124,
	118125,
	118126,
	118127,
	118128,
	118129,
	118130,
	118131,
	118132,
	118133,
	118134,
	118135,
	118136,
	118137,
	118138,
	118139,
	118140,
	118141,
	118142,
	118143,
	118144,
	118145,
	118146,
	118147,
	118148,
	118149,
	118150,
	118151,
	118152,
	118153,
	118154,
	118155,
	118156,
	118157,
	118158,
	118159,
	118160,
	118161,
	118162,
	118163,
	118164,
	118166,
	118167,
	118168,
	118169,
	118170,
	118172,
	118173,
	118176,
	118177,
	118178,
	118180,
	118181,
	118182,
	118183,
	118184,
	118185,
	118186,
	118187,
	118188,
	118192,
	118193,
	118194,
	118195,
	118196,
	118197,
	118198,
	118209,
	118210,
	118212,
	118217,
	118218,
	118219,
	118220,
	118221,
	118222,
	118227,
	118232,
	118233,
	118234,
	118237,
	118238,
	118239,
	118240,
	118241,
	118242,
	118243,
	118246,
	118247,
	118248,
	118249,
	118250,
	118251,
	118252,
	118253,
	118254,
	118255,
	118256,
	118257,
	118258,
	118259,
	118260,
	118261,
	118262,
	118263,
	118264,
	118265,
	118266,
	118267,
	118268,
	118269,
	118270,
	118271,
	118272,
	118273,
	118274,
	118275,
	118276,
	118277,
	118278,
	118279,
	118280,
	118281,
	118282,
	118283,
	118284,
	118285,
	118286,
	118287,
	118288,
	118289,
	118290,
	118291,
	118292,
	118293,
	118294,
	118295,
	118296,
	118297,
	118298,
	118299,
	118300,
	118301,
	118302,
	118303,
	118304,
	118305,
	118306,
	118307,
	118308,
	118309,
	118310,
	118311,
	118312,
	118313,
	118314,
	118315,
	118316,
	118317,
	118318,
	118319,
	118320,
	118321,
	118322,
	118323,
	118324,
	118325,
	118326,
	118327,
	118328,
	118329,
	118330,
	118331,
	118332,
	118333,
	118334,
	118335,
	118336,
	118337,
	118338,
	118339,
	118340,
	118341,
	118342,
	118343,
	118344,
	118345,
	118346,
	118347,
	118348,
	118349,
	118350,
	118351,
	118352,
	118353,
	118354,
	118355,
	118356,
	118357,
	118358,
	118359,
	118360,
	118361,
	118362,
	118363,
	118364,
	118365,
	118366,
	118367,
	118368,
	118369,
	118370,
	118371,
	118372,
	118373,
	118374,
	118375,
	118376,
	118377,
	118378,
	118379,
	118380,
	118381,
	118382,
	118383,
	118384,
	118385,
	118386,
	118387,
	118388,
	118389,
	118390,
	118391,
	118392,
	118393,
	118394,
	118395,
	118396,
	118397,
	118398,
	118399,
	118400,
	118401,
	118402,
	118403,
	118404,
	118405,
	118406,
	118407,
	118408,
	118409,
	118410,
	118411,
	118412,
	118413,
	118414,
	118415,
	118416,
	118417,
	118418,
	118419,
	118420,
	118421,
	118422,
	118423,
	118424,
	118425,
	118426,
	118427,
	118428,
	118429,
	118430,
	118431,
	118432,
	118433,
	118434,
	118435,
	118436,
	118437,
	118438,
	118439,
	118440,
	118441,
	118442,
	118443,
	118444,
	118445,
	118446,
	118447,
	118448,
	118449,
	118450,
	118451,
	118452,
	118453,
	118454,
	118455,
	118456,
	118457,
	118458,
	118459,
	118460,
	118461,
	118462,
	118463,
	118464,
	118465,
	118466,
	118467,
	118468,
	118469,
	118470,
	118471,
	118472,
	118473,
	118474,
	118475,
	118476,
	118477,
	118478,
	118479,
	118480,
	118481,
	118482,
	118483,
	118484,
	118485,
	118486,
	118487,
	118488,
	118489,
	118490,
	118491,
	118492,
	118493,
	118494,
	118495,
	118496,
	118497,
	118498,
	118499,
	118500,
	118501,
	118502,
	118503,
	118504,
	118505,
	118506,
	118507,
	118508,
	118509,
	118510,
	118511,
	118512,
	118513,
	118514,
	118515,
	118516,
	118517,
	118518,
	118519,
	118520,
	118521,
	118522,
	118523,
	118524,
	118525,
	118526,
	118527,
	118528,
	118529,
	118530,
	118531,
	118532,
	118533,
	118534,
	118535,
	118536,
	118537,
	118538,
	118539,
	118540,
	118541,
	118542,
	118543,
	118544,
	118545,
	118546,
	118547,
	118548,
	118549,
	118550,
	118551,
	118552,
	118553,
	118554,
	118555,
	118556,
	118557,
	118558,
	118559,
	118560,
	118561,
	118562,
	118563,
	118564,
	118565,
	118566,
	118567,
	118568,
	118569,
	118570,
	118571,
	118572,
	118573,
	118574,
	118575,
	118576,
	118577,
	118578,
	118579,
	118580,
	118581,
	118582,
	118583,
	118584,
	118585,
	118586,
	118587,
	118588,
	118589,
	118590,
	118591,
	118592,
	118593,
	118594,
	118595,
	118596,
	118597,
	118598,
	118599,
	118600,
	118601,
	118602,
	118603,
	118604,
	118605,
	118606,
	118607,
	118608,
	118609,
	118610,
	118611,
	118612,
	118613,
	118614,
	118615,
	118616,
	118617,
	118618,
	118619,
	118620,
	118621,
	118622,
	118623,
	118624,
	118625,
	118626,
	118627,
	118628,
	118629,
	118630,
	118631,
	118632,
	118633,
	118634,
	118635,
	118636,
	118637,
	118638,
	118639,
	118640,
	118641,
	118642,
	118643,
	118644,
	118645,
	118646,
	118647,
	118648,
	118649,
	118650,
	118651,
	118652,
	118653,
	118654,
	118655,
	118656,
	118657,
	118658,
	118659,
	118660,
	118661,
	118662,
	118663,
	118664,
	118665,
	118666,
	118667,
	118668,
	118669,
	118670,
	118671,
	118672,
	118673,
	118674,
	118675,
	118676,
	118677,
	118678,
	118679,
	118680,
	118681,
	118682,
	118683,
	118684,
	118685,
	118686,
	118687,
	118688,
	118689,
	118690,
	118691,
	118692,
	118693,
	118694,
	118695,
	118696,
	118697,
	118698,
	118699,
	118700,
	118701,
	118702,
	118703,
	118704,
	118705,
	118706,
	118707,
	118708,
	118709,
	118710,
	118711,
	118712,
	118713,
	118714,
	118715,
	118716,
	118717,
	118718,
	118719,
	118720,
	118721,
	118722,
	118723,
	118724,
	118725,
	118726,
	118727,
	118728,
	118729,
	118730,
	118731,
	118732,
	118733,
	118734,
	118735,
	118736,
	118737,
	118738,
	118739,
	118740,
	118741,
	118742,
	118743,
	118744,
	118745,
	118746,
	118747,
	118748,
	118749,
	118750,
	118751,
	118752,
	118753,
	118754,
	118755,
	118756,
	118757,
	118758,
	118759,
	118760,
	118761,
	118762,
	118763,
	118764,
	118765,
	118766,
	118767,
	118768,
	118769,
	118770,
	118771,
	118772,
	118773,
	118774,
	118775,
	118776,
	118777,
	118778,
	118779,
	118780,
	118781,
	118782,
	118783,
	118784,
	118785,
	118786,
	118787,
	118788,
	118789,
	118790,
	118791,
	118792,
	118793,
	118794,
	118795,
	118796,
	118797,
	118798,
	118799,
	118800,
	118801,
	118802,
	118803,
	118804,
	118805,
	118806,
	118807,
	118808,
	118809,
	118810,
	118811,
	118812,
	118813,
	118814,
	118815,
	118816,
	118817,
	118818,
	118819,
	118820,
	118821,
	118822,
	118824,
	118829,
	118830,
	118831,
	118832,
	118833,
	118834,
	118835,
	118836,
	118837,
	118838,
	118839,
	118840,
	118841,
	118842,
	118843,
	118844,
	118845,
	118846,
	118847,
	118851,
	118852,
	118853,
	118854,
	118855,
	118868,
	118869,
	118873,
	118875,
	118876,
	118877,
	118878,
	118879,
	118880,
	118881,
	118882,
	118883,
	118884,
	118885,
	118886,
	118887,
	118888,
	118889,
	118890,
	118891,
	118892,
	118893,
	118894,
	118895,
	118896,
	118897,
	118898,
	118899,
	118900,
	118901,
	118902,
	118903,
	118904,
	118905,
	118906,
	118907,
	118908,
	118909,
	118910,
	118911,
	118912,
	118913,
	118914,
	118915,
	118916,
	118917,
	118918,
	118919,
	118920,
	118921,
	118922,
	118923,
	118924,
	118925,
	118926,
	118927,
	118928,
	118929,
	118930,
	118931,
	118932,
	118933,
	118934,
	118935,
	118936,
	118937,
	118938,
	118939,
	118940,
	118941,
	118942,
	118943,
	118944,
	118945,
	118946,
	118947,
	118948,
	118949,
	118950,
	118951,
	118952,
	118953,
	118954,
	118955,
	118956,
	118957,
	118958,
	118959,
	118960,
	118961,
	118962,
	118963,
	118964,
	118965,
	118966,
	118967,
	118968,
	118969,
	118970,
	118971,
	118972,
	118973,
	118974,
	118975,
	118976,
	118977,
	118978,
	118979,
	118980,
	118981,
	118982,
	118983,
	118984,
	118985,
	118986,
	118987,
	118988,
	118989,
	118990,
	118991,
	118992,
	118993,
	118994,
	118995,
	118996,
	118997,
	118998,
	118999,
	119000,
	119001,
	119002,
	119003,
	119004,
	119005,
	119006,
	119007,
	119008,
	119009,
	119010,
	119011,
	119012,
	119013,
	119014,
	119015,
	119016,
	119017,
	119018,
	119019,
	119020,
	119021,
	119022,
	119023,
	119024,
	119025,
	119026,
	119027,
	119028,
	119029,
	119030,
	119031,
	119032,
	119033,
	119034,
	119035,
	119036,
	119037,
	119038,
	119039,
	119040,
	119041,
	119042,
	119043,
	119044,
	119045,
	119046,
	119047,
	119048,
	119049,
	119050,
	119051,
	119052,
	119053,
	119054,
	119055,
	119056,
	119057,
	119058,
	119059,
	119060,
	119061,
	119062,
	119063,
	119064,
	119065,
	119066,
	119067,
	119068,
	119069,
	119070,
	119071,
	119072,
	119073,
	119074,
	119075,
	119076,
	119077,
	119078,
	119079,
	119080,
	119081,
	119082,
	119083,
	119084,
	119085,
	119086,
	119087,
	119088,
	119089,
	119090,
	119091,
	119092,
	119093,
	119094,
	119095,
	119096,
	119097,
	119098,
	119099,
	119100,
	119101,
	119102,
	119103,
	119104,
	119105,
	119106,
	119107,
	119108,
	119109,
	119110,
	119111,
	119112,
	119113,
	119114,
	119115,
	119116,
	119117,
	119118,
	119119,
	119120,
	119121,
	119122,
	119123,
	119124,
	119125,
	119126,
	119127,
	119128,
	119129,
	119130,
	119131,
	119132,
	119133,
	119134,
	119135,
	119136,
	119137,
	119138,
	119139,
	119140,
	119141,
	119142,
	119143,
	119144,
	119145,
	119146,
	119147,
	119148,
	119149,
	119150,
	119151,
	119152,
	119153,
	119154,
	119155,
	119156,
	119157,
	119158,
	119159,
	119160,
	119161,
	119162,
	119163,
	119164,
	119165,
	119166,
	119167,
	119168,
	119169,
	119170,
	119171,
	119172,
	119173,
	119174,
	119175,
	119176,
	119177,
	119178,
	119179,
	119180,
	119181,
	119182,
	119183,
	119184,
	119185,
	119186,
	119187,
	119188,
	119189,
	119190,
	119191,
	119192,
	119193,
	119194,
	119195,
	119196,
	119197,
	119198,
	119199,
	119200,
	119201,
	119202,
	119203,
	119204,
	119205,
	119206,
	119207,
	119208,
	119209,
	119210,
	119211,
	119212,
	119213,
	119214,
	119215,
	119216,
	119217,
	119218,
	119219,
	119220,
	119221,
	119222,
	119223,
	119224,
	119225,
	119226,
	119227,
	119228,
	119229,
	119230,
	119231,
	119232,
	119233,
	119234,
	119235,
	119236,
	119237,
	119238,
	119239,
	119240,
	119241,
	119242,
	119243,
	119244,
	119245,
	119246,
	119247,
	119248,
	119249,
	119250,
	119251,
	119252,
	119253,
	119254,
	119255,
	119256,
	119257,
	119258,
	119259,
	119260,
	119261,
	119262,
	119263,
	119264,
	119265,
	119266,
	119267,
	119268,
	119269,
	119270,
	119271,
	119272,
	119273,
	119274,
	119275,
	119276,
	119277,
	119278,
	119279,
	119280,
	119281,
	119282,
	119283,
	119284,
	119285,
	119286,
	119287,
	119288,
	119289,
	119290,
	119291,
	119292,
	119293,
	119294,
	119295,
	119296,
	119297,
	119298,
	119299,
	119300,
	119301,
	119302,
	119303,
	119304,
	119305,
	119306,
	119307,
	119308,
	119309,
	119310,
	119311,
	119312,
	119313,
	119314,
	119315,
	119316,
	119317,
	119318,
	119319,
	119320,
	119321,
	119322,
	119323,
	119324,
	119325,
	119326,
	119327,
	119328,
	119329,
	119330,
	119331,
	119332,
	119333,
	119334,
	119335,
	119336,
	119337,
	119338,
	119339,
	119340,
	119341,
	119342,
	119343,
	119344,
	119345,
	119346,
	119347,
	119349,
	119350,
	119351,
	119352,
	119354,
	119355,
	119356,
	119357,
	119358,
	119359,
	119360,
	119361,
	119362,
	119363,
	119364,
	119365,
	119366,
	119367,
	119373,
	119374,
	119375,
	119381,
	119382,
	119383,
	119384,
	119385,
	119390,
	119392,
	119393,
	119394,
	119398,
	119399,
	119401,
	119402,
	119403,
	119404,
	119405,
	119406,
	119407,
	119408,
	119409,
	119411,
	119412,
	119413,
	119414,
	119415,
	119416,
	119417,
	119418,
	119419,
	119420,
	119421,
	119422,
	119423,
	119424,
	119425,
	119426,
	119427,
	119428,
	119429,
	119430,
	119431,
	119432,
	119433,
	119434,
	119435,
	119436,
	119437,
	119438,
	119439,
	119440,
	119441,
	119442,
	119443,
	119444,
	119445,
	119446,
	119447,
	119448,
	119449,
	119450,
	119451,
	119452,
	119453,
	119454,
	119455,
	119456,
	119457,
	119458,
	119459,
	119460,
	119461,
	119462,
	119463,
	119464,
	119465,
	119466,
	119467,
	119468,
	119469,
	119470,
	119471,
	119472,
	119473,
	119474,
	119476,
	119477,
	119478,
	119479,
	119480,
	119481,
	119482,
	119483,
	119484,
	119485,
	119486,
	119487,
	119488,
	119489,
	119490,
	119491,
	119492,
	119493,
	119494,
	119495,
	119496,
	119497,
	119498,
	119499,
	119500,
	119501,
	119502,
	119503,
	119504,
	119505,
	119506,
	119507,
	119508,
	119509,
	119510,
	119511,
	119512,
	119513,
	119514,
	119515,
	119516,
	119517,
	119518,
	119519,
	119520,
	119521,
	119522,
	119523,
	119524,
	119525,
	119526,
	119527,
	119528,
	119529,
	119530,
	119531,
	119532,
	119533,
	119534,
	119535,
	119536,
	119537,
	119538,
	119539,
	119540,
	119541,
	119542,
	119543,
	119544,
	119545,
	119546,
	119547,
	119548,
	119549,
	119550,
	119551,
	119552,
	119553,
	119554,
	119555,
	119556,
	119557,
	119558,
	119559,
	119560,
	119561,
	119562,
	119563,
	119564,
	119565,
	119566,
	119567,
	119568,
	119569,
	119570,
	119571,
	119572,
	119573,
	119574,
	119575,
	119576,
	119577,
	119578,
	119579,
	119580,
	119581,
	119582,
	119583,
	119584,
	119585,
	119586,
	119587,
	119588,
	119589,
	119590,
	119591,
	119592,
	119593,
	119594,
	119595,
	119596,
	119597,
	119598,
	119599,
	119600,
	119601,
	119602,
	119603,
	119604,
	119605,
	119606,
	119607,
	119608,
	119609,
	119610,
	119611,
	119612,
	119613,
	119614,
	119615,
	119616,
	119617,
	119618,
	119619,
	119620,
	119621,
	119622,
	119623,
	119624,
	119625,
	119626,
	119627,
	119628,
	119629,
	119630,
	119631,
	119632,
	119633,
	119634,
	119635,
	119636,
	119637,
	119638,
	119639,
	119640,
	119641,
	119642,
	119643,
	119644,
	119645,
	119646,
	119647,
	119648,
	119649,
	119650,
	119651,
	119652,
	119653,
	119654,
	119655,
	119656,
	119657,
	119658,
	119659,
	119660,
	119661,
	119662,
	119663,
	119664,
	119665,
	119666,
	119667,
	119668,
	119669,
	119670,
	119671,
	119672,
	119673,
	119674,
	119675,
	119676,
	119677,
	119678,
	119679,
	119680,
	119681,
	119682,
	119683,
	119684,
	119685,
	119686,
	119687,
	119688,
	119689,
	119690,
	119691,
	119692,
	119693,
	119694,
	119695,
	119696,
	119697,
	119698,
	119699,
	119700,
	119701,
	119702,
	119703,
	119704,
	119705,
	119706,
	119707,
	119708,
	119709,
	119710,
	119711,
	119712,
	119713,
	119714,
	119715,
	119716,
	119717,
	119718,
	119719,
	119720,
	119721,
	119722,
	119723,
	119724,
	119725,
	119726,
	119727,
	119728,
	119729,
	119730,
	119731,
	119732,
	119733,
	119734,
	119735,
	119736,
	119737,
	119738,
	119739,
	119740,
	119741,
	119742,
	119743,
	119744,
	119745,
	119746,
	119747,
	119748,
	119749,
	119750,
	119751,
	119752,
	119753,
	119754,
	119755,
	119756,
	119757,
	119758,
	119759,
	119760,
	119761,
	119762,
	119763,
	119764,
	119765,
	119766,
	119767,
	119768,
	119769,
	119770,
	119771,
	119772,
	119773,
	119774,
	119775,
	119776,
	119777,
	119778,
	119779,
	119780,
	119781,
	119782,
	119783,
	119784,
	119785,
	119786,
	119787,
	119788,
	119789,
	119790,
	119791,
	119792,
	119793,
	119794,
	119795,
	119796,
	119797,
	119798,
	119799,
	119800,
	119801,
	119802,
	119803,
	119804,
	119805,
	119806,
	119807,
	119808,
	119809,
	119810,
	119811,
	119812,
	119813,
	119814,
	119815,
	119816,
	119817,
	119818,
	119819,
	119820,
	119821,
	119822,
	119823,
	119824,
	119825,
	119826,
	119827,
	119828,
	119829,
	119830,
	119831,
	119832,
	119833,
	119834,
	119835,
	119836,
	119837,
	119838,
	119839,
	119840,
	119841,
	119842,
	119843,
	119844,
	119845,
	119846,
	119847,
	119848,
	119849,
	119850,
	119851,
	119852,
	119853,
	119854,
	119855,
	119856,
	119857,
	119858,
	119859,
	119860,
	119861,
	119862,
	119863,
	119864,
	119865,
	119866,
	119867,
	119868,
	119869,
	119870,
	119871,
	119872,
	119873,
	119874,
	119875,
	119876,
	119877,
	119878,
	119879,
	119880,
	119881,
	119882,
	119883,
	119884,
	119885,
	119886,
	119887,
	119888,
	119889,
	119890,
	119891,
	119892,
	119893,
	119894,
	119895,
	119896,
	119897,
	119898,
	119899,
	119900,
	119901,
	119902,
	119903,
	119904,
	119905,
	119906,
	119907,
	119908,
	119909,
	119910,
	119911,
	119912,
	119913,
	119914,
	119915,
	119916,
	119917,
	119918,
	119919,
	119920,
	119921,
	119922,
	119923,
	119924,
	119925,
	119926,
	119927,
	119928,
	119929,
	119930,
	119931,
	119932,
	119933,
	119934,
	119935,
	119936,
	119937,
	119938,
	119939,
	119940,
	119941,
	119942,
	119943,
	119944,
	119945,
	119946,
	119947,
	119948,
	119949,
	119950,
	119951,
	119952,
	119953,
	119954,
	119955,
	119956,
	119957,
	119958,
	119959,
	119960,
	119961,
	119962,
	119963,
	119964,
	119965,
	119966,
	119967,
	119968,
	119969,
	119970,
	119971,
	119972,
	119973,
	119974,
	119975,
	119976,
	119977,
	119978,
	119979,
	119980,
	119981,
	119982,
	119984,
	119985,
	119986,
	119987,
	119988,
	119989,
	119990,
	119991,
	119992,
	119993,
	119994,
	120007,
	120009,
	120010,
	120011,
	120012,
	120013,
	120015,
	120017,
	120018,
	120019,
	120020,
	120029,
	120032,
	120033,
}

func TerminateSectorCmd(getActorAddress ActorAddressGetter) *cli.Command {
	return &cli.Command{
		Name:      "terminate",
		Usage:     "Forcefully terminate a sector (WARNING: This means losing power and pay a one-time termination penalty(including collateral) for the terminated sector)",
		ArgsUsage: "[sectorNum1 sectorNum2 ...]",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "actor",
				Usage: "specify the address of miner actor",
			},
			&cli.BoolFlag{
				Name:  "really-do-it",
				Usage: "pass this flag if you know what you are doing",
			},
			&cli.StringFlag{
				Name:  "from",
				Usage: "specify the address to send the terminate message from",
			},
		},
		Action: func(cctx *cli.Context) error {
			if cctx.NArg() < 1 {
				return lcli.ShowHelp(cctx, fmt.Errorf("at least one sector must be specified"))
			}

			var maddr address.Address
			if act := cctx.String("actor"); act != "" {
				var err error
				maddr, err = address.NewFromString(act)
				if err != nil {
					return fmt.Errorf("parsing address %s: %w", act, err)
				}
			}

			if !cctx.Bool("really-do-it") {
				return fmt.Errorf("this is a command for advanced users, only use it if you are sure of what you are doing")
			}

			nodeApi, closer, err := lcli.GetFullNodeAPI(cctx)
			if err != nil {
				return err
			}
			defer closer()

			ctx := lcli.ReqContext(cctx)

			if maddr.Empty() {
				maddr, err = getActorAddress(cctx)
				if err != nil {
					return err
				}
			}

			sectorNumbers := tsns

			confidence := uint64(cctx.Int("confidence"))

			var fromAddr address.Address
			if from := cctx.String("from"); from != "" {
				var err error
				fromAddr, err = address.NewFromString(from)
				if err != nil {
					return fmt.Errorf("parsing address %s: %w", from, err)
				}
			} else {
				mi, err := nodeApi.StateMinerInfo(ctx, maddr, types.EmptyTSK)
				if err != nil {
					return err
				}

				fromAddr = mi.Worker
			}
			smsg, err := TerminateSectors(ctx, nodeApi, maddr, sectorNumbers, fromAddr)
			if err != nil {
				return err
			}

			wait, err := nodeApi.StateWaitMsg(ctx, smsg.Cid(), confidence)
			if err != nil {
				return err
			}

			if wait.Receipt.ExitCode.IsError() {
				return fmt.Errorf("terminate sectors message returned exit %d", wait.Receipt.ExitCode)
			}
			return nil
		},
	}
}

func TerminateSectorCmd1(getActorAddress ActorAddressGetter) *cli.Command {
	return &cli.Command{
		Name:      "terminate",
		Usage:     "Forcefully terminate a sector (WARNING: This means losing power and pay a one-time termination penalty(including collateral) for the terminated sector)",
		ArgsUsage: "[sectorNum1 sectorNum2 ...]",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "actor",
				Usage: "specify the address of miner actor",
			},
			&cli.BoolFlag{
				Name:  "really-do-it",
				Usage: "pass this flag if you know what you are doing",
			},
			&cli.StringFlag{
				Name:  "from",
				Usage: "specify the address to send the terminate message from",
			},
		},
		Action: func(cctx *cli.Context) error {
			if cctx.NArg() < 1 {
				return lcli.ShowHelp(cctx, fmt.Errorf("at least one sector must be specified"))
			}

			var maddr address.Address
			if act := cctx.String("actor"); act != "" {
				var err error
				maddr, err = address.NewFromString(act)
				if err != nil {
					return fmt.Errorf("parsing address %s: %w", act, err)
				}
			}

			if !cctx.Bool("really-do-it") {
				return fmt.Errorf("this is a command for advanced users, only use it if you are sure of what you are doing")
			}

			nodeApi, closer, err := lcli.GetFullNodeAPI(cctx)
			if err != nil {
				return err
			}
			defer closer()

			ctx := lcli.ReqContext(cctx)

			if maddr.Empty() {
				maddr, err = getActorAddress(cctx)
				if err != nil {
					return err
				}
			}

			var outerErr error
			sectorNumbers := lo.Map(cctx.Args().Slice(), func(sn string, _ int) int {
				sectorNum, err := strconv.Atoi(sn)
				if err != nil {
					outerErr = fmt.Errorf("could not parse sector number: %w", err)
					return 0
				}
				return sectorNum
			})
			if outerErr != nil {
				return outerErr
			}

			confidence := uint64(cctx.Int("confidence"))

			var fromAddr address.Address
			if from := cctx.String("from"); from != "" {
				var err error
				fromAddr, err = address.NewFromString(from)
				if err != nil {
					return fmt.Errorf("parsing address %s: %w", from, err)
				}
			} else {
				mi, err := nodeApi.StateMinerInfo(ctx, maddr, types.EmptyTSK)
				if err != nil {
					return err
				}

				fromAddr = mi.Worker
			}
			smsg, err := TerminateSectors(ctx, nodeApi, maddr, sectorNumbers, fromAddr)
			if err != nil {
				return err
			}

			wait, err := nodeApi.StateWaitMsg(ctx, smsg.Cid(), confidence)
			if err != nil {
				return err
			}

			if wait.Receipt.ExitCode.IsError() {
				return fmt.Errorf("terminate sectors message returned exit %d", wait.Receipt.ExitCode)
			}
			return nil
		},
	}
}

type TerminatorNode interface {
	StateSectorPartition(ctx context.Context, maddr address.Address, sectorNumber abi.SectorNumber, tok types.TipSetKey) (*miner.SectorLocation, error)
	MpoolPushMessage(ctx context.Context, msg *types.Message, spec *api.MessageSendSpec) (*types.SignedMessage, error)
}

func TerminateSectors(ctx context.Context, full TerminatorNode, maddr address.Address, sectorNumbers []int, fromAddr address.Address) (*types.SignedMessage, error) {

	terminationMap := make(map[string]*miner2.TerminationDeclaration)

	for _, sectorNum := range sectorNumbers {

		sectorbit := bitfield.New()
		sectorbit.Set(uint64(sectorNum))

		loca, err := full.StateSectorPartition(ctx, maddr, abi.SectorNumber(sectorNum), types.EmptyTSK)
		if err != nil {
			return nil, fmt.Errorf("get state sector partition %s", err)
		}

		key := fmt.Sprintf("%d-%d", loca.Deadline, loca.Partition)
		if td, exists := terminationMap[key]; exists {
			td.Sectors.Set(uint64(sectorNum))
		} else {
			terminationMap[key] = &miner2.TerminationDeclaration{
				Deadline:  loca.Deadline,
				Partition: loca.Partition,
				Sectors:   sectorbit,
			}
		}
	}

	terminationDeclarationParams := make([]miner2.TerminationDeclaration, 0, len(terminationMap))
	for _, td := range terminationMap {
		terminationDeclarationParams = append(terminationDeclarationParams, *td)
	}

	terminateSectorParams := &miner2.TerminateSectorsParams{
		Terminations: terminationDeclarationParams,
	}

	sp, errA := actors.SerializeParams(terminateSectorParams)
	if errA != nil {
		return nil, xerrors.Errorf("serializing params: %w", errA)
	}

	smsg, err := full.MpoolPushMessage(ctx, &types.Message{
		From:   fromAddr,
		To:     maddr,
		Method: builtin.MethodsMiner.TerminateSectors,

		Value:  big.Zero(),
		Params: sp,
	}, nil)
	if err != nil {
		return nil, xerrors.Errorf("mpool push message: %w", err)
	}

	fmt.Println("sent termination message:", smsg.Cid())

	return smsg, nil
}
