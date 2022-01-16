

scrubs = {
    "weblinks"  : r"\bhttps\S+|\bwww\S+",
    "locs"      : r"\bLocation.{,20}",
    "salaries"  : r"\bSalary|\bSALARY|£[^A-Za-z]+\.",
    "benefits"  : r"\bBenefits.+\.|\bbenefits.+",
    "html tags" : r"html",
    "gunk"      : r"\uf0a7|\xa0|\b'{0,1}ll\b",
    "footnotes" : r"Additional Information.+$|Eligible.+\.",
    "Filler1"    : r"[rR]ole|Job (Description|Reference|Type|Title)|About Us",
    "Filler2"   : r"\bdays\b|\bwork\S{0,}",
    "Filler3"   : r"\bteam\b|\b[eE]xperience\b|([sS]e|[jJ]u)nior",
    "Filler4"   : r"\bskills\b|\blooking\b|\bbusiness\b|\bjoin\b|\bopportunity\b",
    "Filler5"   : r"\bnew\b|Apply Now",
    "HR Bullshit": "Growth Mindset|Team Player"
  }

roles = {


    "Cloud Engineer": r"\baws\b|\bAWS\b|[aA]zure|[cC]loud ([aA]rchitect|[eE]ngineer)|Data.+Storage|[mM]odel{1,2}er|[dD]ata\s+[sS]olution|Migration",
    "Data Engineer": r"[dD]ata.+[eE]ngineer\S{0,}|\bETL\b|\bBIG DATA\b|Big Data|Hadoop|Cloudera|\bSpark\b",
    "DB or Systems Admin": r"[aA]dmin.{0,1}|\b(IT|Tech) Support|[sS]ervice [dD]esk",
    "Data Scientist" : r"[dD]ata [sS]cien\S+|Machine Learn.+|M[lL]",
    "Software Engineer": r"[sS]oftware|\b[aA]pp\b|([bB]ack|[fF]ront)end|[fF]ull [sS]tack|[kK]afka|(\bC[+#]{0,2}|Ruby|Swift|Java|Python|Scala|Perl|Rust|Julia|\bR\b) (Developer|Engineer)",
    "Analyst" : r"[aA]nalys\S+|[iI]nsight|[rR]esearch|[aA]ssistant|\bBI\b" ,

    "Management": r"\bProject Manage+.|[dD]elivery Lead|[mM]anage(r|ment)|\b[hH]ead of|[dD]irector",
    "N/A" : "9999999999"

}
