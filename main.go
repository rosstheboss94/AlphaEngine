package main

import (
	"embed"
	"log"

	"github.com/wailsapp/wails/v2"
	"github.com/wailsapp/wails/v2/pkg/options"
	"github.com/wailsapp/wails/v2/pkg/options/assetserver"
)

//go:embed all:app/dist
var assets embed.FS

func main() {
	app := &App{}
	if err := wails.Run(&options.App{
		OnStartup: app.startup, Bind: []interface{}{app},
		Title: "Backtest Engine", Width: 1500, Height: 940,
		MinWidth: 1000, MinHeight: 700, Frameless: true,
		BackgroundColour: &options.RGBA{R: 20, G: 28, B: 32, A: 255},
		AssetServer:      &assetserver.Options{Assets: assets},
	}); err != nil {
		log.Fatal(err)
	}
}
