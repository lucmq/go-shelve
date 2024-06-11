package diskvd_test

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/lucmq/go-shelve/shelve"
	diskvd "go-shelve/driver/db/diskv"
)

var StoragePath = filepath.Join(os.TempDir(), "game-test", "db")

type Player struct {
	Name  string
	Level int
	Gold  int
	Items []string
}

type Config struct {
	Difficulty string
}

// NewShelf creates a customized Shelf using Diskv and JSON.
func NewShelf[V any](path string) (*shelve.Shelf[string, V], error) {
	path = filepath.Join(StoragePath, path)
	extension := "json" // Extension of the record files

	db, err := diskvd.NewDefault(path, extension)
	if err != nil {
		return nil, err
	}

	return shelve.Open[string, V](
		path,
		shelve.WithDatabase(db),
		shelve.WithCodec(shelve.JSONCodec()),
	)
}

func Example() {
	// Open the shelf with custom options
	players, _ := NewShelf[Player]("players")
	config, _ := NewShelf[Config]("config")

	defer players.Close()
	defer config.Close()

	// Create the game data
	player := Player{
		Name:  "Frodo",
		Level: 14,
		Gold:  9999,
		Items: []string{"Sting", "Lembas"},
	}
	cfg := Config{
		Difficulty: "Hard",
	}

	// Save the data. Serialization and persistence will be
	// handled automatically by the Shelf.
	players.Put(player.Name, player)
	config.Put("config", cfg)

	// The app storage will contain readable JSON files with
	// configuration and game state, that can be retrieved
	// back to a Go type:
	value, ok, _ := players.Get("Frodo")
	fmt.Println(ok, value.Name, value.Items)

	// Output: true Frodo [Sting Lembas]
}
