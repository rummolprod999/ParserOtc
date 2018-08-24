namespace OTC

open MySql.Data.MySqlClient
open Newtonsoft.Json
open Newtonsoft.Json.Linq
open System
open System.Collections.Generic
open System.Data
open System.Globalization
open System.IO
open System.Linq
open System.Net
open System.Text
open System.Text.RegularExpressions
open System.Threading
open System.Threading.Tasks

module Logging = 
    let mutable FileLog = ""
    
    type Log() = 
        static member logger ([<ParamArray>] args : Object []) = 
            let mutable s = ""
            s <- DateTime.Now.ToString()
            args |> Seq.iter (fun x -> (s <- x.ToString() |> sprintf "%s %s" s))
            (*for arg in args do
                   s <-  arg.ToString() |>  sprintf "%s %s" s*)
            use sw = new StreamWriter(FileLog, true, Encoding.Default)
            sw.WriteLine(s)

module Download = 
    type TimedWebClient() = 
        inherit WebClient()
        override this.GetWebRequest(address : Uri) = 
            let wr = base.GetWebRequest(address)
            wr.Timeout <- 600000
            wr
    
    let DownloadString url = 
        let mutable s = null
        let count = ref 0
        let mutable continueLooping = true
        while continueLooping do
            try 
                //let t ():string = (new TimedWebClient()).DownloadString(url: Uri)
                let task = Task.Run(fun () -> (new TimedWebClient()).DownloadString(url : string))
                if task.Wait(TimeSpan.FromSeconds(650.)) then 
                    s <- task.Result
                    continueLooping <- false
                else raise <| new TimeoutException()
            with _ -> 
                if !count >= 100 then 
                    Logging.Log.logger (sprintf "Не удалось скачать %s xml за %d попыток" url !count)
                    continueLooping <- false
                else incr count
                Thread.Sleep(5000)
        s

module Tools = 
    let TenderKwords (con : MySqlConnection) (idTender : int) (stn : Setting.T) : unit = 
        let resString = new StringBuilder()
        let selectPurObj = 
            sprintf 
                "SELECT DISTINCT po.name, po.okpd_name FROM %spurchase_object AS po LEFT JOIN %slot AS l ON l.id_lot = po.id_lot WHERE l.id_tender = @id_tender" 
                stn.Prefix stn.Prefix
        let cmd1 = new MySqlCommand(selectPurObj, con)
        cmd1.Prepare()
        cmd1.Parameters.AddWithValue("@id_tender", idTender) |> ignore
        let dt = new DataTable()
        let adapter = new MySqlDataAdapter()
        adapter.SelectCommand <- cmd1
        adapter.Fill(dt) |> ignore
        if dt.Rows.Count > 0 then 
            let distrDt = dt.Rows
            for row in distrDt do
                let name = 
                    match row.IsNull("name") with
                    | true -> ""
                    | false -> string <| row.["name"]
                
                let okpdName = 
                    match row.IsNull("okpd_name") with
                    | true -> ""
                    | false -> string <| row.["okpd_name"]
                
                resString.Append(sprintf "%s %s " name okpdName) |> ignore
        let selectAttach = sprintf "SELECT DISTINCT file_name FROM %sattachment WHERE id_tender = @id_tender" stn.Prefix
        let cmd2 = new MySqlCommand(selectAttach, con)
        cmd2.Prepare()
        cmd2.Parameters.AddWithValue("@id_tender", idTender) |> ignore
        let dt2 = new DataTable()
        let adapter2 = new MySqlDataAdapter()
        adapter2.SelectCommand <- cmd2
        adapter2.Fill(dt2) |> ignore
        if dt2.Rows.Count > 0 then 
            let distrDt = dt2.Rows
            for row in distrDt do
                let attName = 
                    match row.IsNull("file_name") with
                    | true -> ""
                    | false -> string <| row.["file_name"]
                resString.Append(sprintf " %s" attName) |> ignore
        let idOrg = ref 0
        let selectPurInf = 
            sprintf "SELECT purchase_object_info, id_organizer FROM %stender WHERE id_tender = @id_tender" stn.Prefix
        let cmd3 = new MySqlCommand(selectPurInf, con)
        cmd3.Prepare()
        cmd3.Parameters.AddWithValue("@id_tender", idTender) |> ignore
        let dt3 = new DataTable()
        let adapter3 = new MySqlDataAdapter()
        adapter3.SelectCommand <- cmd3
        adapter3.Fill(dt3) |> ignore
        if dt3.Rows.Count > 0 then 
            for row in dt3.Rows do
                let purOb = 
                    match row.IsNull("purchase_object_info") with
                    | true -> ""
                    | false -> string <| row.["purchase_object_info"]
                idOrg := match row.IsNull("id_organizer") with
                         | true -> 0
                         | false -> row.["id_organizer"] :?> int
                resString.Append(sprintf " %s" purOb) |> ignore
        match (!idOrg) <> 0 with
        | true -> 
            let selectOrg = 
                sprintf "SELECT full_name, inn FROM %sorganizer WHERE id_organizer = @id_organizer" stn.Prefix
            let cmd4 = new MySqlCommand(selectOrg, con)
            cmd4.Prepare()
            cmd4.Parameters.AddWithValue("@id_organizer", !idOrg) |> ignore
            let dt4 = new DataTable()
            let adapter4 = new MySqlDataAdapter()
            adapter4.SelectCommand <- cmd4
            adapter4.Fill(dt4) |> ignore
            if dt4.Rows.Count > 0 then 
                for row in dt4.Rows do
                    let innOrg = 
                        match row.IsNull("inn") with
                        | true -> ""
                        | false -> string <| row.["inn"]
                    
                    let nameOrg = 
                        match row.IsNull("full_name") with
                        | true -> ""
                        | false -> string <| row.["full_name"]
                    
                    resString.Append(sprintf " %s %s" innOrg nameOrg) |> ignore
        | false -> ()
        let selectCustomer = 
            sprintf 
                "SELECT DISTINCT cus.inn, cus.full_name FROM %scustomer AS cus LEFT JOIN %spurchase_object AS po ON cus.id_customer = po.id_customer LEFT JOIN %slot AS l ON l.id_lot = po.id_lot WHERE l.id_tender = @id_tender" 
                stn.Prefix stn.Prefix stn.Prefix
        let cmd6 = new MySqlCommand(selectCustomer, con)
        cmd6.Prepare()
        cmd6.Parameters.AddWithValue("@id_tender", idTender) |> ignore
        let dt5 = new DataTable()
        let adapter5 = new MySqlDataAdapter()
        adapter5.SelectCommand <- cmd6
        adapter5.Fill(dt5) |> ignore
        if dt5.Rows.Count > 0 then 
            let distrDt = dt5.Rows
            for row in distrDt do
                let innC = 
                    match row.IsNull("inn") with
                    | true -> ""
                    | false -> string <| row.["inn"]
                
                let fullNameC = 
                    match row.IsNull("full_name") with
                    | true -> ""
                    | false -> string <| row.["full_name"]
                
                resString.Append(sprintf " %s %s" innC fullNameC) |> ignore
        let mutable resS = Regex.Replace(resString.ToString(), @"\s+", " ")
        resS <- (resS.Trim())
        let updateTender = 
            sprintf "UPDATE %stender SET tender_kwords = @tender_kwords WHERE id_tender = @id_tender" stn.Prefix
        let cmd5 = new MySqlCommand(updateTender, con)
        cmd5.Prepare()
        cmd5.Parameters.AddWithValue("@id_tender", idTender) |> ignore
        cmd5.Parameters.AddWithValue("@tender_kwords", resS) |> ignore
        let res = cmd5.ExecuteNonQuery()
        if res <> 1 then Logging.Log.logger ("Не удалось обновить tender_kwords", idTender)
        ()
    
    let AddVerNumber (con : MySqlConnection) (pn : string) (stn : Setting.T) : unit = 
        let verNum = ref 1
        let selectTenders = 
            sprintf 
                "SELECT id_tender FROM %stender WHERE purchase_number = @purchaseNumber AND type_fz = 10 ORDER BY UNIX_TIMESTAMP(date_version) ASC" 
                stn.Prefix
        let cmd1 = new MySqlCommand(selectTenders, con)
        cmd1.Prepare()
        cmd1.Parameters.AddWithValue("@purchaseNumber", pn) |> ignore
        let dt1 = new DataTable()
        let adapter1 = new MySqlDataAdapter()
        adapter1.SelectCommand <- cmd1
        adapter1.Fill(dt1) |> ignore
        if dt1.Rows.Count > 0 then 
            let updateTender = 
                sprintf "UPDATE %stender SET num_version = @num_version WHERE id_tender = @id_tender" stn.Prefix
            for ten in dt1.Rows do
                let idTender = (ten.["id_tender"] :?> int)
                let cmd2 = new MySqlCommand(updateTender, con)
                cmd2.Prepare()
                cmd2.Parameters.AddWithValue("@id_tender", idTender) |> ignore
                cmd2.Parameters.AddWithValue("@num_version", !verNum) |> ignore
                cmd2.ExecuteNonQuery() |> ignore
                incr verNum
        ()
    
    let TestInt(t : JToken) : int = 
        match t with
        | null -> 0
        | _ -> (int) t
    
    let TestFloat(t : JToken) : float = 
        match t with
        | null -> 0.
        | _ -> (float) t
    
    let TestDecimal(t : JToken) : decimal = 
        match t with
        | null -> 0.0m
        | _ -> 
            let d = (decimal) t
            Decimal.Round(d, 2)
    
    let TestString(t : JToken) : string = 
        match t with
        | null -> ""
        | _ -> ((string) t).Trim()
    
    let TestStringNull(t : string) : string = 
        match t with
        | null -> ""
        | _ -> ((string) t).Trim('"')
    
    let TestDate(t : string) : DateTime = 
        match t with
        | null | "null" -> DateTime.MinValue
        | _ -> 
            try 
                DateTime.ParseExact(((string) t).Trim('"'), "dd.MM.yyyy (HH:mm)", CultureInfo.InvariantCulture)
            with ex -> DateTime.MinValue
    
    let ClearString(s : string) : string = Regex.Replace(s.ToString(), @"\s+", " ")
    
    let GetOkei(s : string) : string = 
        try 
            let regex = new Regex(@"\((\w+|\D+)\)")
            let matches = regex.Matches(s)
            match matches.Count with
            | x when x > 0 -> matches.[0].Groups.[1].Value
            | _ -> ""
        with ex -> ""
    
    let GetOkpds(jt : JToken) : string * string = 
        try 
            let okpdO = jt :?> JObject
            let t = okpdO.ToObject<Dictionary<string, string>>()
            let okpdN = TestStringNull <| (t.Keys).FirstOrDefault()
            let okpd = TestStringNull <| (t.Values).FirstOrDefault()
            (okpdN, okpd)
        with ex -> ("", "")
    
    let (|Int|_|) str = 
        match System.Int32.TryParse(str) with
        | (true, int) -> Some(int)
        | _ -> None
    
    let GetOkpdGroup(s : string) : int * string = 
        let mutable okpd2GroupCode = 0
        let mutable okpd2GroupLevel1Code = ""
        if s.Length > 1 then 
            let t = s.Substring(0, 2)
            match t with
            | Int i -> okpd2GroupCode <- i
            | _ -> okpd2GroupCode <- 0
        else ()
        if s.Length > 3 then 
            if s.IndexOf(".") <> -1 then okpd2GroupLevel1Code <- s.Substring(3, 1)
            else ()
        else ()
        (okpd2GroupCode, okpd2GroupLevel1Code)
    
    let GetConformity(s : string) : int = 
        let sLower = s.ToLower()
        match sLower with
        | s when s.Contains("открыт") -> 5
        | s when s.Contains("аукцион") -> 1
        | s when s.Contains("котиров") -> 2
        | s when s.Contains("предложен") -> 3
        | s when s.Contains("единств") -> 4
        | _ -> 6
    
    let GetRegionString(s: string) : string =
        let sLower = s.ToLower()
        match sLower with
        | s when s.Contains("отсуств") -> ""
        | s when s.Contains("белгор") -> "белгор"
        | s when s.Contains("брянск") -> "брянск"
        | s when s.Contains("владимир") -> "владимир"
        | s when s.Contains("воронеж") -> "воронеж"
        | s when s.Contains("иванов") -> "иванов"
        | s when s.Contains("калужск") -> "калужск"
        | s when s.Contains("костром") -> "костром"
        | s when s.Contains("курск") -> "курск"
        | s when s.Contains("липецк") -> "липецк"
        | s when s.Contains("москва") -> "москва"
        | s when s.Contains("московск") -> "московск"
        | s when s.Contains("орлов") -> "орлов"
        | s when s.Contains("рязан") -> "рязан"
        | s when s.Contains("смолен") -> "смолен"
        | s when s.Contains("тамбов") -> "тамбов"
        | s when s.Contains("твер") -> "твер"
        | s when s.Contains("тульс") -> "тульс"
        | s when s.Contains("яросл") -> "яросл"
        | s when s.Contains("архан") -> "архан"
        | s when s.Contains("вологод") -> "вологод"
        | s when s.Contains("калинин") -> "калинин"
        | s when s.Contains("карел") -> "карел"
        | s when s.Contains("коми") -> "коми"
        | s when s.Contains("ленинг") -> "ленинг"
        | s when s.Contains("мурм") -> "мурм"
        | s when s.Contains("ненец") -> "ненец"
        | s when s.Contains("новгор") -> "новгор"
        | s when s.Contains("псков") -> "псков"
        | s when s.Contains("санкт") -> "санкт"
        | s when s.Contains("адыг") -> "адыг"
        | s when s.Contains("астрахан") -> "астрахан"
        | s when s.Contains("волгог") -> "волгог"
        | s when s.Contains("калмык") -> "калмык"
        | s when s.Contains("краснод") -> "краснод"
        | s when s.Contains("ростов") -> "ростов"
        | s when s.Contains("дагест") -> "дагест"
        | s when s.Contains("ингуш") -> "ингуш"
        | s when s.Contains("кабардин") -> "кабардин"
        | s when s.Contains("карача") -> "карача"
        | s when s.Contains("осети") -> "осети"
        | s when s.Contains("ставроп") -> "ставроп"
        | s when s.Contains("чечен") -> "чечен"
        | s when s.Contains("башкор") -> "башкор"
        | s when s.Contains("киров") -> "киров"
        | s when s.Contains("марий") -> "марий"
        | s when s.Contains("мордов") -> "мордов"
        | s when s.Contains("нижегор") -> "нижегор"
        | s when s.Contains("оренбур") -> "оренбур"
        | s when s.Contains("пензен") -> "пензен"
        | s when s.Contains("пермс") -> "пермс"
        | s when s.Contains("самар") -> "самар"
        | s when s.Contains("сарат") -> "сарат"
        | s when s.Contains("татарс") -> "татарс"
        | s when s.Contains("удмурт") -> "удмурт"
        | s when s.Contains("ульян") -> "ульян"
        | s when s.Contains("чуваш") -> "чуваш"
        | s when s.Contains("курган") -> "курган"
        | s when s.Contains("свердлов") -> "свердлов"
        | s when s.Contains("тюмен") -> "тюмен"
        | s when s.Contains("ханты") -> "ханты"
        | s when s.Contains("челяб") -> "челяб"
        | s when s.Contains("ямало") -> "ямало"
        | s when s.Contains("алтайск") -> "алтайск"
        | s when s.Contains("алтай") -> "алтай"
        | s when s.Contains("бурят") -> "бурят"
        | s when s.Contains("забайк") -> "забайк"
        | s when s.Contains("иркут") -> "иркут"
        | s when s.Contains("кемеров") -> "кемеров"
        | s when s.Contains("краснояр") -> "краснояр"
        | s when s.Contains("новосиб") -> "новосиб"
        | s when s.Contains("томск") -> "томск"
        | s when s.Contains("омск") -> "омск"
        | s when s.Contains("тыва") -> "тыва"
        | s when s.Contains("хакас") -> "хакас"
        | s when s.Contains("амурск") -> "амурск"
        | s when s.Contains("еврей") -> "еврей"
        | s when s.Contains("камчат") -> "камчат"
        | s when s.Contains("магад") -> "магад"
        | s when s.Contains("примор") -> "примор"
        | s when s.Contains("сахалин") -> "сахалин"
        | s when s.Contains("якут") -> "саха"
        | s when s.Contains("саха") -> "саха"
        | s when s.Contains("хабар") -> "хабар"
        | s when s.Contains("чукот") -> "чукот"
        | s when s.Contains("крым") -> "крым"
        | s when s.Contains("севастоп") -> "севастоп"
        | s when s.Contains("байкон") -> "байкон"
        | _ -> ""