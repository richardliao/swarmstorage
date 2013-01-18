package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"text/template"
	"unicode"
	"unicode/utf8"
)

// A Command is an implementation of a swarm command.
type Command struct {
	// Run runs the command.
	// The args are the arguments after the command name.
	Run func(cmd *Command, args []string)

	// UsageLine is the one-line usage message.
	// The first word in the line is taken to be the command name.
	UsageLine string

	// Short is the short description shown in the 'swarm help' output.
	Short string

	// Long is the long message shown in the 'swarm help <this-command>' output.
	Long string

	// Flag is a set of flags specific to this command.
	Flag flag.FlagSet

	// CustomFlags indicates that the command will do its own flag parsing.
	CustomFlags bool
}

// Name returns the command's name: the first word in the usage line.
func (c *Command) Name() string {
	name := c.UsageLine
	i := strings.Index(name, " ")
	if i >= 0 {
		name = name[:i]
	}
	return name
}

func (c *Command) Usage() {
	fmt.Fprintf(os.Stderr, "%s\n", strings.TrimSpace(c.Long))
	fmt.Fprintf(os.Stderr, "Usage: %s\n\n", c.UsageLine)
	os.Exit(2)
}

// Runnable reports whether the command can be run; otherwise
// it is a documentation pseudo-command such as importpath.
func (c *Command) Runnable() bool {
	return c.Run != nil
}

var usageTemplate = `This is the tool for managing swarm.

Usage:

	swarm command [arguments]

The commands are:
{{range .}}{{if .Runnable}}
    {{.Name | printf "%-11s"}} {{.Short}}{{end}}{{end}}

Use "swarm help [command]" for more information about a command.

Additional help topics:
{{range .}}{{if not .Runnable}}
    {{.Name | printf "%-11s"}} {{.Short}}{{end}}{{end}}

Use "swarm help [topic]" for more information about that topic.

`

var helpTemplate = `{{.Long | trim}}

{{if .Runnable}}Usage: swarm {{.UsageLine}}{{end}}
`

// tmpl executes the given template text on data, writing the result to w.
func tmpl(w io.Writer, text string, data interface{}) {
	t := template.New("top")
	t.Funcs(template.FuncMap{"trim": strings.TrimSpace, "capitalize": capitalize})
	template.Must(t.Parse(text))
	if err := t.Execute(w, data); err != nil {
		panic(err)
	}
}

func capitalize(s string) string {
	if s == "" {
		return s
	}
	r, n := utf8.DecodeRuneInString(s)
	return string(unicode.ToTitle(r)) + s[n:]
}

func printUsage(w io.Writer) {
	tmpl(w, usageTemplate, commands)
}

func usage() {
	printUsage(os.Stderr)
	os.Exit(2)
}

// help implements the 'help' command.
func help(args []string) {
	if len(args) == 0 {
		printUsage(os.Stdout)
		// not exit 2: succeeded at 'swarm help'.
		return
	}
	if len(args) != 1 {
		fmt.Fprintf(os.Stderr, "Usage: swarm help command\n\nToo many arguments given.\n")
		os.Exit(2) // failed at 'swarm help'
	}

	arg := args[0]

	for _, cmd := range commands {
		if cmd.Name() == arg {
			tmpl(os.Stdout, helpTemplate, cmd)
			fmt.Fprintf(os.Stderr, "\nOptions:\n\n")
			cmd.Flag.PrintDefaults()
			fmt.Fprintf(os.Stderr, "\n")
			// not exit 2: succeeded at 'swarm help cmd'.
			return
		}
	}

	fmt.Fprintf(os.Stderr, "Unknown help topic %#q.  Run 'swarm help'.\n", arg)
	os.Exit(2) // failed at 'swarm help cmd'
}

// Commands lists the available commands and help topics.
// The order here is the order in which they are printed by 'swarm help'.
var commands = []*Command{
	cmdVersion,
	cmdDomaininit,
	cmdDaemonize,
	cmdDeviceinit,
	cmdNodeinit,
	cmdServe,
}
