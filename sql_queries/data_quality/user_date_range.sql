select
    date_trunc('month', max("createdDate")) as max_month 
    , date_trunc('month', min("createdDate")) as min_month 
from "fetch".users