## Download and pre-process ACS data 

## Load libraries
library(tidyverse) 
library(magrittr)
library(tidycensus)

#install.packages("magrittr") # package installations are only needed the first time you use it
#install.packages("dplyr")    # alternative installation of the %>%
library(magrittr) # needs to be run every time you start R and want to use %>%
library(dplyr)    # alternatively, this also loads %>%

## REFERENCE: https://walker-data.com/tidycensus/

## Define API
census_api_key("bcf88461fe70a6fd6b75262dcc087d1272228eef")

all_states <- c('AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA','HI','ID','IL',
                'IN','IA','KS','KY','LA','ME','MD','MA','MI','MN','MS','MO','MT',
                'NE','NV','NH','NJ','NM','NY','NC','ND','OH','OK','OR','PA','RI',
                'SC','SD','TN','TX','UT','VT','VA','WA','WV','WI','WY')

## AGE AND SEX
get_age_table <- function(MY_STATE){
  
    st.ab <- MY_STATE
    yr <- 2021
    geog <- "county"
    dat <- "acs5"
    
    ## Get list of all variables
    vars <- load_variables(year = yr, dataset = dat)
    
    ## Retrieve Age table 
    age <- get_acs(survey = dat, year = yr, geography = geog, state = st.ab,
                   table = "B01001", cache_table = TRUE)
    
    age <- age %>% select(-NAME)
    age <- age %>% select(-moe) %>% spread(variable, estimate)
    
    ## Get Total Pop, Male, Female
    names(age)[2] <- "POP"
    names(age)[3] <- "POP_M"
    names(age)[27] <- "POP_F"
    
    ## Combine Male and Female populations
    age[,4:26] <- age[,4:26] + age[,28:50]
    
    ## Rename columns
    names(age)[4:26] <- paste0("POP_A", c("0004","0509","1014","1517","1819","20",
                                          "21","2224","2529","3034","3539","4044",
                                          "4549","5054","5559","6061","6264","6566",
                                          "6769","7074","7579","8084","85p"))
    
    ## Subset, reorder
    age %<>% select(GEOID:POP_M, POP_F, POP_A0004:POP_A85p)
    
    ## Quick check
    sum(age$POP != rowSums(age[,3:4]))
    sum(age$POP != rowSums(age[,5:27]))
    
    ## Write out file
    # write_csv(age, paste0("Desktop/acs_results/age_covs/",
                          # st.ab, "_", yr, "_",str_replace_all(geog, " ", ""),
                          # "_", dat, "_", yr, "_","age_sex.csv")) 
    write_csv(age, paste0("Desktop/acs_results/",st.ab, "/",
                          yr, "_",str_replace_all(geog, " ", ""),
                          "_", dat, "_", yr, "_","age_sex.csv")) 
  
}
## RACE
get_race_tables <- function(MY_STATE){
  
  st.ab <- MY_STATE
  
  race <- get_acs(survey = dat, year = yr, geography = geog, state = MY_STATE,
                  table = "B03002")
  
  race <- race %>% select(-NAME, -moe)
  race <- race %>% spread(variable, estimate)
  
  names(race)[2] <- "POP"
  names(race)[3] <- "POP_NH"
  names(race)[13] <- "POP_HISP"
  
  eth_only <- race[,c(1:3,13)]
  race_eth <- race[,c(1:2,4:13)]
  race_eth$B03002_008 <- race_eth$B03002_008 + race_eth$B03002_009
  
  ## Rename columns
  names(race_eth)[3:8] <- paste0("POP_", c("WHITENH","BLACKNH","AIANNH",
                                           "ASIANNH","NHPINH","OTH2PLNH"))
  
  race_eth <- race_eth[,c(1:8,12)]
  race[,4:12] <- race[,4:12] + race[,14:22]
  race$B03002_008 <- race$B03002_008 + race$B03002_009
  
  ## Rename columns
  names(race)[4:9] <- paste0("POP_", c("WHITE","BLACK","AIAN","ASIAN",
                                       "NHPI","OTH2PL"))
  
  race <- race[,c(1:2,4:9)]
  
  ## Quick checks
  sum(eth_only$POP != rowSums(eth_only[,3:4]))
  sum(race_eth$POP != rowSums(race_eth[,3:9]))
  sum(race$POP != rowSums(race[,3:8]))
  
  ## Write out files
  
  
  write_csv(eth_only, paste0("Desktop/acs_results/",st.ab, "/",
                             yr, "_",str_replace_all(geog, " ", ""),
                             "_", dat, "_", yr, "_","ethnicity.csv")) 
  write_csv(race_eth, paste0("Desktop/acs_results/",st.ab, "/",
                             yr, "_",str_replace_all(geog, " ", ""),
                             "_", dat, "_", yr, "_","race-ethnicity.csv")) 
  write_csv(race, paste0("Desktop/acs_results/",st.ab, "/",
                         yr, "_",str_replace_all(geog, " ", ""),
                         "_", dat, "_", yr, "_","race.csv")) 
}
## INCOME
get_income_table <- function(MY_STATE){
  
  st.ab <- MY_STATE
  
  inc <- get_acs(survey = dat, year = yr, geography = geog, state = st.ab,
                 table = "B19001")
  inc <- inc %>% select(c("GEOID", "variable", "estimate"))
  inc <- inc %>% spread(variable, estimate)
  names(inc)[2] <- "HH"
  names(inc)[3:18] <- paste0("HHI_", c("00_10k","10_15k","15_20k","20_25k",
                                       "25_30k","30_35k","35_40k","40_45k",
                                       "45_50k","50_60k","60_75k","75_100k",
                                       "100_125k","125_150k","150_200k",
                                       "200k_p"))
  sum(inc$HH != rowSums(inc[,3:18]))
  med_inc <- get_acs(survey = dat, year = yr, geography = geog, state = st.ab,
                     table = "B19013")
  med_inc <- med_inc %>% select(c("GEOID", "variable", "estimate"))
  med_inc <- med_inc %>% spread(variable, estimate)
  names(med_inc)[2] <- "MHI"
  med_inc <- merge(med_inc, inc, by = "GEOID")
  
  write_csv(med_inc, paste0("Desktop/acs_results/",st.ab, "/",
                            yr, "_",str_replace_all(geog, " ", ""),
                            "_", dat, "_", yr, "_","income.csv")) 
}

# get_age_table('CA')

# for (each_state in all_states) {
#   get_age_table(each_state)
# }
# 
# for (each_state in all_states) {
#   get_race_tables(each_state)
# }


# 
# for (each_state in all_states) {
#   get_income_table(each_state)
# }


## EDUCATION
get_education_table <- function(MY_STATE){
  
  st.ab <- MY_STATE
  
  edu <- get_acs(survey = dat,
                 year = yr,
                 geography = geog,
                 state = st.ab,
                 table = "B15003")
  
  ## Drop NAME, MOE
  edu <- edu %>% select(c("GEOID", "variable", "estimate"))
  
  ## Convert to wide
  edu <- edu %>% spread(variable, estimate)
  
  ## Rename HH column
  names(edu)[2] <- "POP_A25p"
  
  ## Consolidate and rename
  edu$EDU_ltHS <- rowSums(edu[,3:17])  ## Less than HS
  edu$EDU_HS <- rowSums(edu[,18:21])  ## HS diploma, GED, some college
  names(edu)[22:26] <- c("EDU_ASSOC",
                         "EDU_BACH",
                         "EDU_MAST",
                         "EDU_PROF",
                         "EDU_DOCT")
  
  ## Subset and reorder
  edu <- edu[,c(1,2,27,28,22:26)]
  
  ## Quick check
  sum(edu$POP_A25p != rowSums(edu[,3:9]))
  
  write_csv(edu, paste0("Desktop/acs_results/",st.ab, "/",
                            yr, "_",str_replace_all(geog, " ", ""),
                            "_", dat, "_", yr, "_","education.csv"))
  
}
## HEALTH INSURANCE
get_health_ins_table <- function(MY_STATE){
  
  st.ab <- MY_STATE
  
  hi <- get_acs(survey = dat,
                year = yr,
                geography = geog,
                state = st.ab,
                table = "B27010")
  
  ## Drop NAME, MOE
  hi <- hi %>% select(c("GEOID", "variable", "estimate"))
  
  ## Convert to wide
  hi <- hi %>% spread(variable, estimate)
  
  ## Rename columns and calculate values
  names(hi)[3] <- "POP_A0018"
  names(hi)[19] <- "POP_A1934"
  names(hi)[35] <- "POP_A3564"
  names(hi)[52] <- "POP_A65p"
  hi$HINS_A0018 <- hi$POP_A0018 - hi$B27010_017
  hi$HINS_A1934 <- hi$POP_A1934 - hi$B27010_033
  hi$HINS_A3564 <- hi$POP_A3564 - hi$B27010_050
  hi$HINS_A65p <- hi$POP_A65p - hi$B27010_066
  
  ## Subset and reorder
  hi <- hi[,c(1,3,19,35,52,68:71)]
  
  ## Write out file
  write_csv(hi, paste0("Desktop/acs_results/",st.ab, "/",
                            yr, "_",str_replace_all(geog, " ", ""),
                            "_", dat, "_", yr, "_","healthinsurance.csv"))
}
## OCCUPANCY
get_occupancy_table <- function(MY_STATE){
  
  st.ab <- MY_STATE
  
  occ <- get_acs(survey = dat,
                 year = yr,
                 geography = geog,
                 state = st.ab,
                 table = "B25002")
  
  ## Drop NAME, MOE
  occ <- occ %>% select(c("GEOID", "variable", "estimate"))
  
  ## Convert to wide
  occ <- occ %>% spread(variable, estimate)
  
  ## Rename columns and calculate values
  names(occ)[2:4] <- c("HU",
                       "HU_OCC",
                       "HU_VAC")
  
  ## Retrieve Own/Rent
  occ2 <- get_acs(survey = dat,
                  year = yr,
                  geography = geog,
                  state = st.ab,
                  table = "B25003")
  
  ## Drop NAME, MOE
  occ2 <- occ2 %>% select(c("GEOID", "variable", "estimate"))
  
  ## Convert to wide
  occ2 <- occ2 %>% spread(variable, estimate)
  
  ## Rename columns and calculate values
  names(occ2)[3:4] <- c("HU_OCC_OWN",
                        "HU_OCC_RENT")
  
  ## Retrieve Occupants per room
  occ3 <- get_acs(survey = dat,
                  year = yr,
                  geography = geog,
                  state = st.ab,
                  table = "B25014")
  
  ## Drop NAME, MOE
  occ3 <- occ3 %>% select(c("GEOID", "variable", "estimate"))
  
  ## Convert to wide
  occ3 <- occ3 %>% spread(variable, estimate)
  
  ## Calculate values
  occ3$HU_OCC_OPRlt050 <- occ3$B25014_003 + occ3$B25014_009
  occ3$HU_OCC_OPR051100 <- occ3$B25014_004 + occ3$B25014_010
  occ3$HU_OCC_OPR101150 <- occ3$B25014_005 + occ3$B25014_011
  occ3$HU_OCC_OPR151200 <- occ3$B25014_006 + occ3$B25014_012
  occ3$HU_OCC_OPRgt200 <- occ3$B25014_007 + occ3$B25014_013
  
  ## Merge (and subset)
  occ <- merge(occ, occ2[,c(1,3:4)], by = "GEOID")
  occ <- merge(occ, occ3[,c(1,15:19)], by = "GEOID")
  
  ## Write out file
  write_csv(occ, paste0("Desktop/acs_results/",st.ab, "/",
                            yr, "_",str_replace_all(geog, " ", ""),
                            "_", dat, "_", yr, "_","occupancy.csv"))  
}
## HOUSEHOLD CHARS
get_household_table <- function(MY_STATE){
  
  st.ab <- MY_STATE
  
  hh <- get_acs(survey = dat,
                year = yr,
                geography = geog,
                state = st.ab,
                table = "B11016")
  
  ## Drop NAME, MOE
  hh <- hh %>% select(c("GEOID", "variable", "estimate"))
  
  ## Convert to wide
  hh <- hh %>% spread(variable, estimate)
  
  ## Rename columns and calculate values
  names(hh)[2] <- "HH"
  names(hh)[11] <- "HH_1P"
  hh$HH_2P <- hh$B11016_003 + hh$B11016_011
  hh$HH_3P <- hh$B11016_004 + hh$B11016_012
  hh$HH_4P <- hh$B11016_005 + hh$B11016_013
  hh$HH_5P <- hh$B11016_006 + hh$B11016_014
  hh$HH_6P <- hh$B11016_007 + hh$B11016_015
  hh$HH_7pP <- hh$B11016_008 + hh$B11016_016
  
  ## Subset
  hh <- hh[,c(1,2,11,18:23)]
  
  ## Write out file
  write_csv(hh, paste0("Desktop/acs_results/",st.ab, "/",
                            yr, "_",str_replace_all(geog, " ", ""),
                            "_", dat, "_", yr, "_","householdsize.csv"))
}
## STRUCTURES
get_structures_table <- function(MY_STATE){
  
  st.ab <- MY_STATE
  
  hu <- get_acs(survey = dat,
                year = yr,
                geography = geog,
                state = st.ab,
                table = "B25024")
  
  ## Drop NAME, MOE
  hu <- hu %>% select(c("GEOID", "variable", "estimate"))
  
  ## Convert to wide
  hu <- hu %>% spread(variable, estimate)
  
  ## Rename columns and calculate values
  names(hu)[2:10] <- c("HU",
                       "HU_UIS01D",
                       "HU_UIS01A",
                       "HU_UIS02",
                       "HU_UIS0304",
                       "HU_UIS0509",
                       "HU_UIS1019",
                       "HU_UIS2049",
                       "HU_UIS50P")
  hu$HU_UISOTHER <- hu$B25024_010 + hu$B25024_011
  
  ## Subset
  hu <- hu[,c(1:10,13)]
  
  ## Write out file
  write_csv(hu, paste0("Desktop/acs_results/",st.ab, "/",
                            yr, "_",str_replace_all(geog, " ", ""),
                            "_", dat, "_", yr, "_","unitsinstructure.csv"))
  
}
## OCCUPATION/EMPLOYMENT
get_employment_tables <- function(MY_STATE){
  
  st.ab <- MY_STATE
  
  emp <- get_acs(survey = dat,
                 year = yr,
                 geography = geog,
                 state = st.ab,
                 table = "C24010")
  
  ## Drop NAME, MOE
  emp <- emp %>% select(c("GEOID", "variable", "estimate"))
  
  ## Convert to wide
  emp <- emp %>% spread(variable, estimate)
  
  ## Consolidate M/F columns, subset
  emp[,4:38] <- emp[,4:38] + emp[,40:74]
  emp <- emp[,-c(3,39:74)]
  
  ## Make one table general with 1st level
  ## Make one table detailed with 2nd level
  ## Make one table with v detailed 3rd level
  
  ## Subset and rename first table
  emp_1 <- emp[,c(1,2,3,19,27,30,34)]
  names(emp_1) <- c("GEOID",
                    "POP_16p_EMP",
                    paste0("OCC_",
                           c("MBSA",
                             "SERV",
                             "SALES",
                             "NRCM",
                             "PTMM")))
  
  ## Write out file
  write_csv(emp_1, paste0("Desktop/acs_results/",st.ab, "/",
                          yr, "_",str_replace_all(geog, " ", ""),
                          "_", dat, "_", yr, "_","occupation_level1.csv"))
  
  ## Subset and rename second table
  emp_2 <- emp[,c(1,2,4,7,11,16,20,21,24,25,26,28,29,31,32,33,35,36,37)]
  names(emp_2) <- c("GEOID",
                    "POP_16p_EMP",
                    paste0("OCC_",
                           c("MBSA_MBF",
                             "MBSA_CES",
                             "MBSA_ELCAM",
                             "MBSA_HCPT",  
                             "SERV_HCS",
                             "SERV_PS",
                             "SERV_FPS",
                             "SERV_BGM",
                             "SERV_PCS",
                             "SALES_SR",
                             "SALES_OAS",
                             "NRCM_FFF",
                             "NRCM_CE",
                             "NRCM_IMR",
                             "PTMM_P",
                             "PTMM_T",
                             "PTMM_MM")))
  
  ## Write out file
  write_csv(emp_2, paste0("Desktop/acs_results/",st.ab, "/",
                          yr, "_",str_replace_all(geog, " ", ""),
                          "_", dat, "_", yr, "_","occupation_level2.csv"))
  
  ## Subset and rename second table
  emp_3 <- emp[,c(1,2,5,6,8,9,10,12,13,14,15,17,18,20,22,23,24,25,26,28,29,31,32,33,35,36,37)]
  names(emp_3) <- c("GEOID",
                    "POP_16p_EMP",
                    paste0("OCC_",
                           c("MBSA_MBF_M",
                             "MBSA_MBF_BF",
                             "MBSA_CES_CM",
                             "MBSA_CES_AE",
                             "MBSA_CES_LPSS",
                             "MBSA_ELCAM_CSS",
                             "MBSA_ELCAM_L",
                             "MBSA_ELCAM_EL",
                             "MBSA_ELCAM_ADESM",
                             "MBSA_HCPT_HDTP",
                             "MBSA_HCPT_HTT",  
                             "SERV_HCS",
                             "SERV_PS_FP",
                             "SERV_PS_LE",
                             "SERV_FPS",
                             "SERV_BGM",
                             "SERV_PCS",
                             "SALES_SR",
                             "SALES_OAS",
                             "NRCM_FFF",
                             "NRCM_CE",
                             "NRCM_IMR",
                             "PTMM_P",
                             "PTMM_T",
                             "PTMM_MM")))
  
  # sum(rowSums(emp_3[,3:27]) != emp_3$POP_16p_EMP)
  
  ## Write out file
  write_csv(emp_3, paste0("Desktop/acs_results/",st.ab, "/",
                          yr, "_",str_replace_all(geog, " ", ""),
                          "_", dat, "_", yr, "_","occupation_level3.csv"))
}
## OCCUPATION/INDUSTRY
get_industry_tables <- function(MY_STATE){
  
  st.ab <- MY_STATE
  
  ind <- get_acs(survey = dat,
                 year = yr,
                 geography = geog,
                 state = st.ab,
                 table = "C24030")
  
  ## Drop NAME, MOE
  ind <- ind %>% select(c("GEOID", "variable", "estimate"))
  
  ## Convert to wide
  ind <- ind %>% spread(variable, estimate)
  
  ## Consolidate M/F columns, subset
  ind[,4:29] <- ind[,4:29] + ind[,31:56]
  ind <- ind[,-c(3,30:56)]
  
  ## Make one table general with 1st level
  ## Make one table detailed with 2nd level
  
  ## Subset and rename first table
  ind_1 <- ind[,c(1,2,3,6:10,13,14,17,21,24,27,28)]
  names(ind_1) <- c("GEOID",
                    "POP_16p_EMP",
                    paste0("IND_",
                           c("AFFHM",
                             "CONS",
                             "MAN",
                             "WHT",
                             "RETT",
                             "TWU",
                             "INF",
                             "FIRR",
                             "PSMAW",
                             "EHCSA",
                             "AERAF",
                             "OSER",
                             "PUBA")))
  
  # sum(rowSums(ind_1[,3:15]) != ind_1$POP_16p_EMP)
  
  ## Write out file
  write_csv(ind_1, paste0("Desktop/acs_results/",st.ab, "/",
                            yr, "_",str_replace_all(geog, " ", ""),
                            "_", dat, "_", yr, "_","industry_level1.csv"))
  
  ## Subset and rename first table
  ind_2 <- ind[,c(1,2,4,5,6:9,11,12,13,15,16,18,19,20,22,23,25,26,27,28)]
  names(ind_2) <- c("GEOID",
                    "POP_16p_EMP",
                    paste0("IND_",
                           c("AFFHM_AFFH",
                             "AFFHM_MQE",
                             "CONS",
                             "MAN",
                             "WHT",
                             "RETT",
                             "TWU_TW",
                             "TWU_U",
                             "INF",
                             "FIRR_FI",
                             "FIRR_RR",
                             "PSMAW_PST",
                             "PSMAW_M",
                             "PSMAW_ASWM",
                             "EHCSA_E",
                             "EHCSA_HCSA",
                             "AERAF_AER",
                             "AERAF_AF",
                             "OSER",
                             "PUBA")))
  
  # sum(rowSums(ind_2[,3:22]) != ind_1$POP_16p_EMP)
  
  ## Write out file
  write_csv(ind_2, paste0("Desktop/acs_results/",st.ab, "/",
                          yr, "_",str_replace_all(geog, " ", ""),
                          "_", dat, "_", yr, "_","industry_level2.csv"))
  
}


get_education_table('AK')
get_health_ins_table('AK')
get_occupancy_table('AK')
get_household_table('AK')
get_structures_table('AK')
get_employment_tables('AK')
get_industry_tables('AK')



for (each_state in all_states) {
  get_education_table(each_state)
  get_health_ins_table(each_state)
  get_occupancy_table(each_state)
  get_household_table(each_state)
  get_structures_table(each_state)
  get_employment_tables(each_state)
  get_industry_tables(each_state)
}

# write_csv(med_inc, paste0("Desktop/acs_results/",st.ab, "/",
#                           yr, "_",str_replace_all(geog, " ", ""),
#                           "_", dat, "_", yr, "_","income.csv"))








## LEFT OFF HERE, 2/20/21

## Mobility 08006, 08008, 08012
## Vehicle ownership?
## Internet/Computers?

# 
# for (each_state in all_states) {
# path <- "Desktop/acs_results/"
# newfolder <- each_state
# dir.create(file.path(dirname(path), newfolder))
# }

