package main

import (
	"fmt"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/filecoin-project/lotus/storage/sealer/storiface"
	"github.com/urfave/cli/v2"
	"strconv"
)

var txstorageCmd = &cli.Command{
	Name:  "txstorage",
	Usage: "Tools for txstorage",
	Flags: []cli.Flag{},
	Subcommands: []*cli.Command{
		showSectorCmd,
		declareSectorCmd,
		dropSectorCmd,
	},
}

var showSectorCmd = &cli.Command{
	Name:      "showSector",
	Usage:     "show sector storage",
	ArgsUsage: "<sectorNum>",
	Flags:     []cli.Flag{},
	Action: func(cctx *cli.Context) error {
		if !cctx.Args().Present() {
			return fmt.Errorf("must specify sector number")
		}

		id, err := strconv.ParseUint(cctx.Args().First(), 10, 64)
		if err != nil {
			return err
		}

		minerApi, closer, err := lcli.GetStorageMinerAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		ctx := lcli.ReqContext(cctx)

		maddr, err := minerApi.ActorAddress(ctx)
		if err != nil {
			return err
		}
		mid, err := address.IDFromAddress(maddr)
		if err != nil {
			return err
		}

		sid := abi.SectorID{
			Number: abi.SectorNumber(id),
			Miner:  abi.ActorID(mid),
		}

		for _, fileType := range storiface.PathTypes {
			ssis, err := minerApi.StorageFindSector(ctx, sid, fileType, 0, false)
			if err != nil {
				return err
			}

			fmt.Printf("fileType (CanSeal, CanStore, Primary):%v\n", fileType)
			for _, ssi := range ssis {
				fmt.Printf("ssi:%v\n", ssi)
			}
			fmt.Printf("\n")
		}

		return nil
	},
}

var declareSectorCmd = &cli.Command{
	Name:      "declareSector",
	Usage:     "declare sector",
	ArgsUsage: "<sectorNum>",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name: "storage",
		},
		&cli.StringFlag{
			Name: "file-type",
		},
		&cli.BoolFlag{
			Name:  "primary",
			Value: false,
		},
	},
	Action: func(cctx *cli.Context) error {
		if !cctx.Args().Present() {
			return fmt.Errorf("must specify sector number")
		}

		id, err := strconv.ParseUint(cctx.Args().First(), 10, 64)
		if err != nil {
			return err
		}

		//
		storageIdStr := cctx.String("storage")
		if storageIdStr == "" {
			return fmt.Errorf("storageId is null")
		}
		storageId := storiface.ID(storageIdStr)

		//
		fileTypeStr := cctx.String("file-type")
		if fileTypeStr == "" {
			return fmt.Errorf("fileType is null")
		}
		ft := storiface.FTNone
		for _, fileType := range storiface.PathTypes {
			if fileType.String() == fileTypeStr {
				ft = fileType
				break
			}
		}
		if ft == storiface.FTNone {
			return fmt.Errorf("fileType is wrong")
		}

		//
		primary := cctx.Bool("primary")

		minerApi, closer, err := lcli.GetStorageMinerAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		ctx := lcli.ReqContext(cctx)

		maddr, err := minerApi.ActorAddress(ctx)
		if err != nil {
			return err
		}
		mid, err := address.IDFromAddress(maddr)
		if err != nil {
			return err
		}

		sid := abi.SectorID{
			Number: abi.SectorNumber(id),
			Miner:  abi.ActorID(mid),
		}

		err = minerApi.StorageDeclareSector(ctx, storageId, sid, ft, primary)
		if err != nil {
			return err
		}

		return nil
	},
}

var dropSectorCmd = &cli.Command{
	Name:      "dropSector",
	Usage:     "drop sector",
	ArgsUsage: "<sectorNum>",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name: "storage",
		},
		&cli.StringFlag{
			Name: "file-type",
		},
	},
	Action: func(cctx *cli.Context) error {
		if !cctx.Args().Present() {
			return fmt.Errorf("must specify sector number")
		}

		id, err := strconv.ParseUint(cctx.Args().First(), 10, 64)
		if err != nil {
			return err
		}

		//
		storageIdStr := cctx.String("storage")
		if storageIdStr == "" {
			return fmt.Errorf("storageId is null")
		}
		storageId := storiface.ID(storageIdStr)

		//
		fileTypeStr := cctx.String("file-type")
		if fileTypeStr == "" {
			return fmt.Errorf("fileType is null")
		}
		ft := storiface.FTNone
		for _, fileType := range storiface.PathTypes {
			if fileType.String() == fileTypeStr {
				ft = fileType
				break
			}
		}
		if ft == storiface.FTNone {
			return fmt.Errorf("fileType is wrong")
		}

		minerApi, closer, err := lcli.GetStorageMinerAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		ctx := lcli.ReqContext(cctx)

		maddr, err := minerApi.ActorAddress(ctx)
		if err != nil {
			return err
		}
		mid, err := address.IDFromAddress(maddr)
		if err != nil {
			return err
		}

		sid := abi.SectorID{
			Number: abi.SectorNumber(id),
			Miner:  abi.ActorID(mid),
		}

		err = minerApi.StorageDropSector(ctx, storageId, sid, ft)
		if err != nil {
			return err
		}

		return nil
	},
}
