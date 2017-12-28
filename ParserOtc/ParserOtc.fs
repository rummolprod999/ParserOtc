namespace OTC

type ParserOtc(stn : Setting.T) =
    let set = stn
    static member tenderCount = ref 0
    member public this.Parsing() =
        ()