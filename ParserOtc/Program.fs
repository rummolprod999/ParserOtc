// Learn more about F# at http://fsharp.org
namespace OTC

open System

module Start = 
    [<EntryPoint>]
    let main argv = 
        let settings = Setting.getSettings()
        let p = Parser(settings)
        p.Parsing()
        0 // return an integer exit code
