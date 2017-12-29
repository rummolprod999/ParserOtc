namespace OTC

open MySql.Data.MySqlClient
open Newtonsoft.Json
open Newtonsoft.Json.Linq
open System
open System.Data
open System.Globalization
open System.IO
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
                "SELECT id_tender FROM %stender WHERE purchase_number = @purchaseNumber AND type_fz = 5 ORDER BY UNIX_TIMESTAMP(date_version) ASC" 
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
    
    let TestString(t : JToken) : string = 
        match t with
        | null -> ""
        | _ -> ((string) t).Trim()
    
    let TestDate(t : string) : DateTime = 
        match t with
        | null | "null" -> DateTime.MinValue
        | _ -> DateTime.Parse(((string) t).Trim('"'))
