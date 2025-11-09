package main

import (
	"errors"
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/goccy/go-yaml"
)

// A Command represents a single step in a batch of changes
type Command interface {
	Name() string

	Execute() error
	Undo() error
}

// Command implementation for moving a file
type CmdMoveFile struct {
	CmdName    string `yaml:"name"`
	SourcePath string `yaml:"source_path"`
	TargetPath string `yaml:"target_path"`
}

func (m *CmdMoveFile) Execute() error {
	target, err := os.OpenFile(m.TargetPath, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer target.Close()

	source, err := os.Open(m.SourcePath)
	if err != nil {
		return err
	}
	defer source.Close()

	_, err = io.Copy(target, source)
	if err != nil {
		return err
	}

	err = os.Remove(source.Name())
	if err != nil {
		return err
	}

	return nil
}
func (m *CmdMoveFile) Undo() error {
	_, err := os.Stat(m.SourcePath)
	sourceExists := !errors.Is(err, os.ErrNotExist)

	_, err = os.Stat(m.TargetPath)
	targetExists := !errors.Is(err, os.ErrNotExist)

	if sourceExists && targetExists {
		err := os.Remove(m.TargetPath)
		if err != nil {
			return err
		}
	} else if sourceExists && !targetExists {
		return nil
	} else if !sourceExists && !targetExists {
		return nil
	} else if !sourceExists && targetExists {

	}
	return nil
}
func (m *CmdMoveFile) Name() string { return m.CmdName }

func NewCmdMoveFile(sourcePath, targetPath string) *CmdMoveFile {
	sourcePath, err := filepath.Abs(sourcePath)
	if err != nil {
		panic(err)
	}

	targetPath, err = filepath.Abs(targetPath)
	if err != nil {
		panic(err)
	}
	return &CmdMoveFile{
		CmdName:    "move",
		SourcePath: sourcePath,
		TargetPath: targetPath,
	}
}

// Command implementation for copying a file
type CmdCopyFile struct {
	CmdName    string `yaml:"name"`
	SourcePath string `yaml:"source_path"`
	TargetPath string `yaml:"target_path"`
}

func (m *CmdCopyFile) Execute() error {
	target, err := os.OpenFile(m.TargetPath, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer target.Close()

	source, err := os.Open(m.SourcePath)
	if err != nil {
		return err
	}
	defer source.Close()

	_, err = io.Copy(target, source)
	if err != nil {
		return err
	}

	return nil
}
func (m *CmdCopyFile) Undo() error {
	err := os.Remove(m.TargetPath)
	if err != nil {
		return err
	}
	return nil
}
func (m *CmdCopyFile) Name() string { return m.CmdName }

func NewCmdCopyFile(sourcePath, targetPath string) *CmdCopyFile {
	sourcePath, err := filepath.Abs(sourcePath)
	if err != nil {
		panic(err)
	}
	targetPath, err = filepath.Abs(targetPath)
	if err != nil {
		panic(err)
	}
	return &CmdCopyFile{
		CmdName:    "copy",
		SourcePath: sourcePath,
		TargetPath: targetPath,
	}
}

type StatusUpdate struct {
	Type   string  `yaml:"type"`
	Action string  `yaml:"action"`
	Index  int     `yaml:"index"`
	Cmd    Command `yaml:"cmd"`
}

func NewStatusUpdate(action string, index int, cmd Command) *StatusUpdate {
	return &StatusUpdate{
		Type:   "status_update",
		Action: action,
		Index:  index,
		Cmd:    cmd,
	}
}

type Batch struct {
	Type     string    `yaml:"type"`
	WalPath  string    `yaml:"wal_path"`
	Commands []Command `yaml:"commands"`
}

func NewBatch(walPath string, commands ...Command) *Batch {
	walPath, err := filepath.Abs(walPath)
	if err != nil {
		panic(err)
	}
	return &Batch{
		Type:     "batch_start",
		WalPath:  walPath,
		Commands: commands,
	}
}

func (b *Batch) ExecuteAll() error {
	_, err := os.Stat(b.WalPath)

	walFile, err := os.OpenFile(b.WalPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	log.Printf("opened WAL at %s", b.WalPath)
	defer walFile.Close()

	batchWalYAML, err := yaml.Marshal([]Batch{*b})
	if err != nil {
		return err
	}

	_, err = walFile.WriteString(string(batchWalYAML))
	if err != nil {
		return err
	}

	err = walFile.Sync()
	if err != nil {
		return err
	}
	log.Println("batch YAML has been written to WAL")

	writeStatus := func(action string, cmd Command, cmdIndex int) error {
		status := NewStatusUpdate(action, cmdIndex, cmd)
		statusYAML, err := yaml.Marshal([]StatusUpdate{*status})
		if err != nil {
			return err
		}

		_, err = walFile.WriteString(string(statusYAML))
		if err != nil {
			return err
		}

		err = walFile.Sync()
		if err != nil {
			return err
		}

		log.Printf("wrote status %q\n", action)
		return nil
	}

	var applied []Command
	for i, cmd := range b.Commands {
		err = cmd.Execute()

		if err != nil {
			log.Printf("command %q failed, undoing operations: %v\n", cmd.Name(), err)
			for i, cmd := range applied {
				err = cmd.Undo()

				if err != nil {
					panic(err)
				}
				err = writeStatus("undone", cmd, i)
				if err != nil {
					panic(err)
				}
				log.Printf("command %q undone\n", cmd.Name())
			}

			return err
		}

		log.Printf("command %q executed\n", cmd.Name())
		applied = append(applied, cmd)

		err = writeStatus("executed", cmd, i)
		if err != nil {
			return err
		}
	}

	err = writeStatus("batch is done", nil, 0)
	if err != nil {
		return err
	}
	return nil
}

func main() {
	batch := NewBatch("wal.yaml", NewCmdMoveFile("a", "b"), NewCmdCopyFile("c", "d"))
	err := batch.ExecuteAll()
	if err != nil {
		panic(err)
	}
}
