namespace OTC

open System
open System.IO

type Parser(s : Setting.T) = 
    
    do 
        if String.IsNullOrEmpty(s.TempPathTenders) || String.IsNullOrEmpty(s.LogPathTenders) then 
            printf "Не получится создать папки для парсинга"
            Environment.Exit(0)
        else 
            match Directory.Exists(s.TempPathTenders) with
            | true -> 
                let dirInfo = new DirectoryInfo(s.TempPathTenders)
                dirInfo.Delete(true)
                Directory.CreateDirectory(s.TempPathTenders) |> ignore
            | false -> Directory.CreateDirectory(s.TempPathTenders) |> ignore
            match Directory.Exists(s.LogPathTenders) with
            | false -> Directory.CreateDirectory(s.LogPathTenders) |> ignore
            | true -> ()
        Logging.FileLog <- sprintf "%s%clog_parsing_otc_%s.log" s.LogPathTenders Path.DirectorySeparatorChar 
                           <| DateTime.Now.ToString("dd_MM_yyyy")
    
    member public this.Parsing() = 
        Logging.Log.logger "Начало парсинга"
        try 
            let Potc = ParserOtc(s)
            Potc.Parsing()
        with ex -> Logging.Log.logger ex
        Logging.Log.logger "Конец парсинга"
        Logging.Log.logger (sprintf "Добавили тендеров %d" !ParserOtc.tenderCount)
