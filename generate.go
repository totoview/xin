// +build ignore

package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"text/template"
)

func initIO(in, out string) (*ast.File, *os.File) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, in, nil, 0)
	if err != nil {
		panic(err)
	}
	o, err := os.Create(out)
	if err != nil {
		panic(err)
	}

	return f, o
}

func collectNames(f *ast.File, prefix string) []string {
	var names []string
	ast.Inspect(f, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.GenDecl:
			for _, spec := range x.Specs {
				if vs, ok := spec.(*ast.ValueSpec); ok && len(vs.Names) > 0 && strings.HasPrefix(vs.Names[0].Name, prefix) {
					if _, ok := vs.Type.(*ast.Ident); ok {
						toks := strings.Split(vs.Names[0].Name, "_")
						if strings.Compare(toks[1], "Unknown") != 0 {
							names = append(names, toks[1])
						}
					}
				}
			}
		}
		return true
	})
	return names
}

func genBaseHelpers() {
	f, out := initIO("pb/base.pb.go", "pb/base.go")
	defer out.Close()

	t := template.Must(template.ParseFiles("templates/base.tpl"))
	t.Execute(out, struct {
		ChannelTypes []string
		UserTypes    []string
	}{collectNames(f, "MsgChannel_"), collectNames(f, "MsgUser_")})
}

func genMgwHelpers() {
	f, out := initIO("pb/mgw/mgw.pb.go", "pb/mgw/mgw.go")
	defer out.Close()

	t := template.Must(template.ParseFiles("templates/mgw.tpl"))
	t.Execute(out, struct {
		RequestTypes []string
		EventTypes   []string
	}{collectNames(f, "MgwRequest_"), collectNames(f, "MgwEvent_")})
}

func genStoreHelpers() {
	f, out := initIO("pb/store/store.pb.go", "pb/store/store.go")
	defer out.Close()

	t := template.Must(template.ParseFiles("templates/store.tpl"))
	t.Execute(out, collectNames(f, "StoreEvent_"))
}

// When working with Kafka, we use protobuf to encode messages. This often involves
// wrapping messages in a generic wrapper message for encoding/decoding:
//
//     message Msg1 {}
//     message Msg2 {}
//     ...
//     message WrapperMsg {
//         enum Type {
//             Msg1 = 1;
//             Msg2 = 2;
//             ...
//         }
//         Type type = 0;
//         Msg1 msg1 = 1;
//         Msg2 msg2 = 2;
//         ...
//     }
//
// This requires us to ensure the value of type match the type of the actual message
// that is wrapped inside. This simple program helps generate utility functions that
// create wrapper messages from wrapped messages.
func main() {
	genBaseHelpers()
	genMgwHelpers()
	genStoreHelpers()
}
