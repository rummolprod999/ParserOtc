namespace OTC

open MySql.Data.MySqlClient
open Newtonsoft.Json
open Newtonsoft.Json.Linq
open System
open System.Collections.Generic
open System.Data
open System.Linq
open System.Text
open System.Text.RegularExpressions
open System.Xml

type ParserOtc(stn : Setting.T) = 
    let set = stn
    let mutable minusDate = 50
    let curDate = DateTime.Now
    let lastDate = curDate.AddDays(-1.)
    let curDateS = curDate.ToString("MM/dd/yyyy", System.Globalization.CultureInfo.InvariantCulture)
    let lastDateS = lastDate.ToString("MM/dd/yyyy", System.Globalization.CultureInfo.InvariantCulture)
    let urlFull = 
        sprintf 
            "https://otc.ru/tenders/api/public/GetTendersExtended?id=%s&DatePublishedFrom=%s&FilterData.PageSize=100&state=1&FilterData.SortingField=2&FilterData.SortingDirection=2" 
            stn.GUID curDateS
    let urlFullLast = 
        sprintf 
            "https://otc.ru/tenders/api/public/GetTendersExtended?id=%s&DatePublishedFrom=%s&FilterData.PageSize=100&state=1&FilterData.SortingField=2&FilterData.SortingDirection=2" 
            stn.GUID lastDateS
    static member val tenderCount = ref 0
    static member typeFz = 10
    
    member public this.ParsingOld() = 
        for i = minusDate downto 1 do
            let dateMinus1 = 
                curDate.AddDays(-1. * (float i))
                       .ToString("MM/dd/yyyy", System.Globalization.CultureInfo.InvariantCulture)
            let dateMinus2 = 
                curDate.AddDays(-1. * ((float i) + 1.))
                       .ToString("MM/dd/yyyy", System.Globalization.CultureInfo.InvariantCulture)
            let urlFullLastOld = 
                sprintf 
                    "https://otc.ru/tenders/api/public/GetTendersExtended?id=%s&DatePublishedFrom=%s&DatePublishedTo=%s&FilterData.PageSize=100&state=1&FilterData.SortingField=2&FilterData.SortingDirection=2" 
                    stn.GUID dateMinus2 dateMinus2
            let lastFOld = 
                sprintf 
                    "https://otc.ru/tenders/api/public/GetTendersExtended?id=%s&DatePublishedFrom=%s&DatePublishedTo=%s&FilterData.PageSize=100&FilterData.PageIndex=%d&state=1&FilterData.SortingField=2&FilterData.SortingDirection=2" 
                    stn.GUID dateMinus2 dateMinus2
            this.ParsinForDate(urlFullLastOld, lastFOld)
        ()
    
    member public this.Parsing() = 
        let lastF = 
            sprintf 
                "https://otc.ru/tenders/api/public/GetTendersExtended?id=%s&DatePublishedFrom=%s&FilterData.PageSize=100&FilterData.PageIndex=%d&state=1&FilterData.SortingField=2&FilterData.SortingDirection=2" 
                stn.GUID lastDateS
        try 
            this.ParsinForDate(urlFullLast, lastF)
        with ex -> Logging.Log.logger ex
        let currF = 
            sprintf 
                "https://otc.ru/tenders/api/public/GetTendersExtended?id=%s&DatePublishedFrom=%s&FilterData.PageSize=100&FilterData.PageIndex=%d&state=1&FilterData.SortingField=2&FilterData.SortingDirection=2" 
                stn.GUID curDateS
        try 
            this.ParsinForDate(urlFull, currF)
        with ex -> Logging.Log.logger ex
        ()
    
    member private this.ParsinForDate(url : string, urlf : int -> string) = 
        //printf "%s" urlFull
        let startPage = Download.DownloadString url
        //printf "%s" startPage
        match startPage with
        | null | "" -> Logging.Log.logger ("Dont get start page", url)
        | s -> 
            let json = JObject.Parse(s)
            let total = Tools.TestInt(json.SelectToken("TotalItems"))
            if total >= 1000 then Logging.Log.logger ("1000 Tenders limit!!!!!", url)
            let countPage = Tools.TestInt(json.SelectToken("TotalPages"))
            for i = 1 to countPage do
                try 
                    this.ParsingPage(i, urlf)
                with ex -> Logging.Log.logger ex
    
    (*[ 1..countPage ] |> List.iter  this.ParsingPage urlf*)
    member private this.ParsingPage(i : int, sF : int -> string) = 
        try 
            let url = sF i
            (*printfn "%s" url*)
            let Page = Download.DownloadString url
            match Page with
            | null -> Logging.Log.logger ("Dont get page", url)
            | s -> 
                let mutable json = null
                try 
                    json <- JObject.Parse(s)
                with ex -> 
                    Logging.Log.logger (ex, s)
                    raise <| System.Exception(ex.Message)
                let items = json.SelectToken("Items")
                if items <> null then 
                    for it in items do
                        try 
                            this.ParsingTender(it, url)
                        with ex -> Logging.Log.logger ex
        with ex -> Logging.Log.logger ex
        ()
    
    member private this.ParsingTender(t : JToken, url : string) = 
        let mutable pNum = Tools.TestString <| t.SelectToken("Number")
        let RegistrationNumber = Tools.TestString <| t.SelectToken("RegistrationNumber")
        if String.IsNullOrEmpty(RegistrationNumber) then 
            if String.IsNullOrEmpty(pNum) then Logging.Log.logger "Empty pNum"
            else 
                use con = new MySqlConnection(set.ConStr)
                con.Open()
                //printfn "%s" (JsonConvert.SerializeObject(t.SelectToken("DatePublished")))
                let DatePublished = Tools.TestDate <| JsonConvert.SerializeObject(t.SelectToken("DatePublished"))
                //printfn "%A" DatePublished
                let dateModified = DatePublished
                let EndDate = Tools.TestDate <| JsonConvert.SerializeObject(t.SelectToken("ApplicationEndDate"))
                let ScoringDate = Tools.TestDate <| JsonConvert.SerializeObject(t.SelectToken("ConsiderationDate"))
                let href = Tools.TestString(t.SelectToken("Url"))
                let selectTend = 
                    sprintf 
                        "SELECT id_tender FROM %stender WHERE id_xml = @id_xml AND date_version = @date_version AND type_fz = @type_fz AND end_date = @end_date AND scoring_date = @scoring_date AND href = @href" 
                        stn.Prefix
                let cmd : MySqlCommand = new MySqlCommand(selectTend, con)
                cmd.Prepare()
                cmd.Parameters.AddWithValue("@id_xml", pNum) |> ignore
                cmd.Parameters.AddWithValue("@date_version", dateModified) |> ignore
                cmd.Parameters.AddWithValue("@type_fz", ParserOtc.typeFz) |> ignore
                cmd.Parameters.AddWithValue("@end_date", EndDate) |> ignore
                cmd.Parameters.AddWithValue("@scoring_date", ScoringDate) |> ignore
                cmd.Parameters.AddWithValue("@href", href) |> ignore
                let reader : MySqlDataReader = cmd.ExecuteReader()
                if reader.HasRows then reader.Close()
                else 
                    reader.Close()
                    let mutable cancelStatus = 0
                    let selectDateT = 
                        sprintf 
                            "SELECT id_tender, date_version, cancel FROM %stender WHERE id_xml = @id_xml AND type_fz = @type_fz AND href = @href" 
                            stn.Prefix
                    let cmd2 = new MySqlCommand(selectDateT, con)
                    cmd2.Prepare()
                    cmd2.Parameters.AddWithValue("@id_xml", pNum) |> ignore
                    cmd2.Parameters.AddWithValue("@type_fz", ParserOtc.typeFz) |> ignore
                    cmd2.Parameters.AddWithValue("@href", href) |> ignore
                    let adapter = new MySqlDataAdapter()
                    adapter.SelectCommand <- cmd2
                    let dt = new DataTable()
                    adapter.Fill(dt) |> ignore
                    for row in dt.Rows do
                        //printfn "%A" <| (row.["date_version"])
                        match dateModified >= ((row.["date_version"]) :?> DateTime) with
                        | true -> row.["cancel"] <- 1
                        | false -> cancelStatus <- 1
                    let commandBuilder = new MySqlCommandBuilder(adapter)
                    commandBuilder.ConflictOption <- ConflictOption.OverwriteChanges
                    adapter.Update(dt) |> ignore
                    let TradeName = Tools.TestString(t.SelectToken("TradeName"))
                    let TenderName = Tools.TestString(t.SelectToken("TenderName"))
                    
                    let PurchaseObjectInfo = 
                        if TradeName = TenderName then Tools.ClearString TenderName
                        else Tools.ClearString <| sprintf "%s %s" TenderName TradeName
                    
                    //printfn "%s" PurchaseObjectInfo
                    let NoticeVersion = ""
                    let Printform = href
                    let IdOrg = ref 0
                    let OrgInn = Tools.TestString(t.SelectToken("Organizer.Inn"))
                    if OrgInn <> "" then 
                        let OrgKpp = Tools.TestString(t.SelectToken("Organizer.Kpp"))
                        let selectOrg = 
                            sprintf "SELECT id_organizer FROM %sorganizer WHERE inn = @inn AND kpp = @kpp" stn.Prefix
                        let cmd3 = new MySqlCommand(selectOrg, con)
                        cmd3.Prepare()
                        cmd3.Parameters.AddWithValue("@inn", OrgInn) |> ignore
                        cmd3.Parameters.AddWithValue("@kpp", OrgKpp) |> ignore
                        let reader = cmd3.ExecuteReader()
                        match reader.HasRows with
                        | true -> 
                            reader.Read() |> ignore
                            IdOrg := reader.GetInt32("id_organizer")
                            reader.Close()
                        | false -> 
                            reader.Close()
                            let OrgName = Tools.TestString(t.SelectToken("Organizer.Name"))
                            let addOrganizer = 
                                sprintf "INSERT INTO %sorganizer SET full_name = @full_name, inn = @inn, kpp = @kpp" 
                                    stn.Prefix
                            let cmd5 = new MySqlCommand(addOrganizer, con)
                            cmd5.Parameters.AddWithValue("@full_name", OrgName) |> ignore
                            cmd5.Parameters.AddWithValue("@inn", OrgInn) |> ignore
                            cmd5.Parameters.AddWithValue("@kpp", OrgKpp) |> ignore
                            cmd5.ExecuteNonQuery() |> ignore
                            IdOrg := int cmd5.LastInsertedId
                    let idPlacingWay = ref 0
                    let placingWayName = Tools.TestString(t.SelectToken("Type"))
                    if placingWayName <> "" then 
                        let selectPlacingWay = 
                            sprintf "SELECT id_placing_way FROM %splacing_way WHERE name= @name" stn.Prefix
                        let cmd6 = new MySqlCommand(selectPlacingWay, con)
                        cmd6.Prepare()
                        cmd6.Parameters.AddWithValue("@name", placingWayName) |> ignore
                        let reader3 = cmd6.ExecuteReader()
                        match reader3.HasRows with
                        | true -> 
                            reader3.Read() |> ignore
                            idPlacingWay := reader3.GetInt32("id_placing_way")
                            reader3.Close()
                        | false -> 
                            reader3.Close()
                            let conf = Tools.GetConformity placingWayName
                            let insertPlacingWay = 
                                sprintf "INSERT INTO %splacing_way SET name= @name, conformity = @conformity" stn.Prefix
                            let cmd7 = new MySqlCommand(insertPlacingWay, con)
                            cmd7.Prepare()
                            cmd7.Parameters.AddWithValue("@name", placingWayName) |> ignore
                            cmd7.Parameters.AddWithValue("@conformity", conf) |> ignore
                            cmd7.ExecuteNonQuery() |> ignore
                            idPlacingWay := int cmd7.LastInsertedId
                    let idEtp = ref 0
                    let etpName = "ЭТП OTC.RU"
                    let etpUrl = "https://tender.otc.ru"
                    if etpName <> "" then 
                        let selectEtp = sprintf "SELECT id_etp FROM %setp WHERE name = @name AND url = @url" stn.Prefix
                        let cmd6 = new MySqlCommand(selectEtp, con)
                        cmd6.Prepare()
                        cmd6.Parameters.AddWithValue("@name", etpName) |> ignore
                        cmd6.Parameters.AddWithValue("@url", etpUrl) |> ignore
                        let reader3 = cmd6.ExecuteReader()
                        match reader3.HasRows with
                        | true -> 
                            reader3.Read() |> ignore
                            idEtp := reader3.GetInt32("id_etp")
                            reader3.Close()
                        | false -> 
                            reader3.Close()
                            let insertEtp = sprintf "INSERT INTO %setp SET name= @name, url= @url, conf=0" stn.Prefix
                            let cmd7 = new MySqlCommand(insertEtp, con)
                            cmd7.Prepare()
                            cmd7.Parameters.AddWithValue("@name", etpName) |> ignore
                            cmd7.Parameters.AddWithValue("@url", etpUrl) |> ignore
                            cmd7.ExecuteNonQuery() |> ignore
                            idEtp := int cmd7.LastInsertedId
                    let numVersion = 0
                    let mutable idRegion = 0
                    let regionS = Tools.TestString(t.SelectToken("Regions"))
                    let regionS = Tools.GetRegionString(regionS)
                    if regionS <> "" then 
                        let selectReg = sprintf "SELECT id FROM %sregion WHERE name LIKE @name" stn.Prefix
                        let cmd46 = new MySqlCommand(selectReg, con)
                        cmd46.Prepare()
                        cmd46.Parameters.AddWithValue("@name", "%" + regionS + "%") |> ignore
                        let reader36 = cmd46.ExecuteReader()
                        match reader36.HasRows with
                        | true -> 
                            reader36.Read() |> ignore
                            idRegion <- reader36.GetInt32("id")
                            reader36.Close()
                        | false -> reader36.Close()
                    let idTender = ref 0
                    (*let insertTender = 
                        sprintf 
                            "INSERT INTO %stender SET id_xml = @id_xml, purchase_number = @purchase_number, doc_publish_date = @doc_publish_date, href = @href, purchase_object_info = @purchase_object_info, type_fz = @type_fz, id_organizer = @id_organizer, id_placing_way = @id_placing_way, id_etp = @id_etp, end_date = @end_date, scoring_date = @scoring_date, bidding_date = @bidding_date, cancel = @cancel, date_version = @date_version, num_version = @num_version, notice_version = @notice_version, xml = @xml, print_form = @print_form" 
                            stn.Prefix*)
                    let insertTender = 
                        String.Format
                            ("INSERT INTO {0}tender SET id_xml = @id_xml, purchase_number = @purchase_number, doc_publish_date = @doc_publish_date, href = @href, purchase_object_info = @purchase_object_info, type_fz = @type_fz, id_organizer = @id_organizer, id_placing_way = @id_placing_way, id_etp = @id_etp, end_date = @end_date, scoring_date = @scoring_date, bidding_date = @bidding_date, cancel = @cancel, date_version = @date_version, num_version = @num_version, notice_version = @notice_version, xml = @xml, print_form = @print_form, id_region = @id_region", 
                             stn.Prefix)
                    let cmd9 = new MySqlCommand(insertTender, con)
                    cmd9.Prepare()
                    cmd9.Parameters.AddWithValue("@id_xml", pNum) |> ignore
                    cmd9.Parameters.AddWithValue("@purchase_number", pNum) |> ignore
                    cmd9.Parameters.AddWithValue("@doc_publish_date", DatePublished) |> ignore
                    cmd9.Parameters.AddWithValue("@href", href) |> ignore
                    cmd9.Parameters.AddWithValue("@purchase_object_info", PurchaseObjectInfo) |> ignore
                    cmd9.Parameters.AddWithValue("@type_fz", ParserOtc.typeFz) |> ignore
                    cmd9.Parameters.AddWithValue("@id_organizer", !IdOrg) |> ignore
                    cmd9.Parameters.AddWithValue("@id_placing_way", !idPlacingWay) |> ignore
                    cmd9.Parameters.AddWithValue("@id_etp", !idEtp) |> ignore
                    cmd9.Parameters.AddWithValue("@end_date", EndDate) |> ignore
                    cmd9.Parameters.AddWithValue("@scoring_date", ScoringDate) |> ignore
                    cmd9.Parameters.AddWithValue("@bidding_date", DateTime.MinValue) |> ignore
                    cmd9.Parameters.AddWithValue("@cancel", cancelStatus) |> ignore
                    cmd9.Parameters.AddWithValue("@date_version", dateModified) |> ignore
                    cmd9.Parameters.AddWithValue("@num_version", numVersion) |> ignore
                    cmd9.Parameters.AddWithValue("@notice_version", NoticeVersion) |> ignore
                    cmd9.Parameters.AddWithValue("@xml", url) |> ignore
                    cmd9.Parameters.AddWithValue("@print_form", Printform) |> ignore
                    cmd9.Parameters.AddWithValue("@id_region", idRegion) |> ignore
                    cmd9.ExecuteNonQuery() |> ignore
                    idTender := int cmd9.LastInsertedId
                    incr ParserOtc.tenderCount
                    let ParticipantFeature = Tools.TestString <| t.SelectToken("ParticipantFeature")
                    let lotNumber = 1
                    let idLot = ref 0
                    let lotMaxPrice = Tools.TestDecimal <| t.SelectToken("Price")
                    let lotCurrency = Tools.TestString <| t.SelectToken("Currency")
                    let insertLot = 
                        sprintf 
                            "INSERT INTO %slot SET id_tender = @id_tender, lot_number = @lot_number, max_price = @max_price, currency = @currency" 
                            stn.Prefix
                    let cmd12 = new MySqlCommand(insertLot, con)
                    cmd12.Parameters.AddWithValue("@id_tender", !idTender) |> ignore
                    cmd12.Parameters.AddWithValue("@lot_number", lotNumber) |> ignore
                    cmd12.Parameters.AddWithValue("@max_price", lotMaxPrice) |> ignore
                    cmd12.Parameters.AddWithValue("@currency", lotCurrency) |> ignore
                    cmd12.ExecuteNonQuery() |> ignore
                    idLot := int cmd12.LastInsertedId
                    if ParticipantFeature <> "" then 
                        let insertrestr = 
                            sprintf "INSERT INTO %srestricts SET id_lot = @id_lot, info = @info" stn.Prefix
                        let cmd30 = new MySqlCommand(insertrestr, con)
                        cmd30.Prepare()
                        cmd30.Parameters.AddWithValue("@id_lot", !idLot) |> ignore
                        cmd30.Parameters.AddWithValue("@info", ParticipantFeature) |> ignore
                        cmd30.ExecuteNonQuery() |> ignore
                    let idCustomer = ref 0
                    let CusInn = Tools.TestString <| t.SelectToken("Customers[0].Inn")
                    if CusInn <> "" then 
                        let selectCustomer = sprintf "SELECT id_customer FROM %scustomer WHERE inn = @inn" stn.Prefix
                        let cmd3 = new MySqlCommand(selectCustomer, con)
                        cmd3.Prepare()
                        cmd3.Parameters.AddWithValue("@inn", CusInn) |> ignore
                        let reader = cmd3.ExecuteReader()
                        match reader.HasRows with
                        | true -> 
                            reader.Read() |> ignore
                            idCustomer := reader.GetInt32("id_customer")
                            reader.Close()
                        | false -> 
                            reader.Close()
                            let insertCustomer = 
                                sprintf 
                                    "INSERT INTO %scustomer SET reg_num = @reg_num, full_name = @full_name, inn = @inn" 
                                    stn.Prefix
                            let RegNum = Guid.NewGuid().ToString()
                            let CusName = Tools.TestString <| t.SelectToken("Customers[0].Name")
                            let cmd14 = new MySqlCommand(insertCustomer, con)
                            cmd14.Prepare()
                            cmd14.Parameters.AddWithValue("@reg_num", RegNum) |> ignore
                            cmd14.Parameters.AddWithValue("@full_name", CusName) |> ignore
                            cmd14.Parameters.AddWithValue("@inn", CusInn) |> ignore
                            cmd14.ExecuteNonQuery() |> ignore
                            idCustomer := int cmd14.LastInsertedId
                    let items = t.SelectToken("TenderItems")
                    if items <> null then 
                        for it in items do
                            let quantity = Tools.TestString <| it.SelectToken("QuantityString")
                            let okei = Tools.GetOkei quantity
                            let okpdNameT = it.SelectToken("Okpd2s")
                            let (okpdName, okpd) = Tools.GetOkpds okpdNameT
                            let (okpd2GroupCode, okpd2GroupLevel1Code) = Tools.GetOkpdGroup okpd
                            let insertLotitem = 
                                sprintf 
                                    "INSERT INTO %spurchase_object SET id_lot = @id_lot, id_customer = @id_customer, okpd2_code = @okpd2_code, okpd_name = @okpd_name, name = @name, quantity_value = @quantity_value, okei = @okei, customer_quantity_value = @customer_quantity_value, okpd2_group_code = @okpd2_group_code, okpd2_group_level1_code = @okpd2_group_level1_code" 
                                    stn.Prefix
                            let cmd19 = new MySqlCommand(insertLotitem, con)
                            cmd19.Prepare()
                            cmd19.Parameters.AddWithValue("@id_lot", !idLot) |> ignore
                            cmd19.Parameters.AddWithValue("@id_customer", !idCustomer) |> ignore
                            cmd19.Parameters.AddWithValue("@okpd2_code", okpd) |> ignore
                            cmd19.Parameters.AddWithValue("@okpd_name", okpdName) |> ignore
                            cmd19.Parameters.AddWithValue("@name", okpdName) |> ignore
                            cmd19.Parameters.AddWithValue("@quantity_value", quantity) |> ignore
                            cmd19.Parameters.AddWithValue("@okei", okei) |> ignore
                            cmd19.Parameters.AddWithValue("@customer_quantity_value", quantity) |> ignore
                            cmd19.Parameters.AddWithValue("@okpd2_group_code", okpd2GroupCode) |> ignore
                            cmd19.Parameters.AddWithValue("@okpd2_group_level1_code", okpd2GroupLevel1Code) |> ignore
                            cmd19.ExecuteNonQuery() |> ignore
                            let deliveryPlace = Tools.TestString <| it.SelectToken("DeliveryAddress")
                            let insertCustomerRequirement = 
                                sprintf 
                                    "INSERT INTO %scustomer_requirement SET id_lot = @id_lot, id_customer = @id_customer, delivery_place = @delivery_place" 
                                    stn.Prefix
                            let cmd16 = new MySqlCommand(insertCustomerRequirement, con)
                            cmd16.Prepare()
                            cmd16.Parameters.AddWithValue("@id_lot", !idLot) |> ignore
                            cmd16.Parameters.AddWithValue("@id_customer", !idCustomer) |> ignore
                            cmd16.Parameters.AddWithValue("@delivery_place", deliveryPlace) |> ignore
                            cmd16.ExecuteNonQuery() |> ignore
                    try 
                        Tools.AddVerNumber con pNum stn
                    with ex -> 
                        Logging.Log.logger "Ошибка добавления версий тендера"
                        Logging.Log.logger ex
                    try 
                        Tools.TenderKwords con (!idTender) stn
                    with ex -> 
                        Logging.Log.logger "Ошибка добавления kwords тендера"
                        Logging.Log.logger ex
        ()
