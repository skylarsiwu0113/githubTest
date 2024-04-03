### Caris 2024 - Q1 2024 - RWD360
### Created on March 21, 2024
## PART 1: Read parquet files

library(dplyr)
library(arrow)
proj_dir <- "/Users/wusx12/Library/CloudStorage/OneDrive-AbbVieInc(O365)/Documents/Projects/RWD/Caris_2024/03_data/Q1_2024/parquet/rwd360/"

## function to extract parquet files from a certain folder
parquet <- function(file_name) {
  output <- list()
  for (i in 1:length(file_name)) {
    s <- read_parquet(file_name[i], as_tibble = T)
    output[[i]] <- s
  }
  output_combine <- do.call(rbind, output) # 120547 * 26
  return(output_combine)
}

######## PART 1: RWD360 xwalk
setwd(file.path(proj_dir, "c3_rwd360_202312_abbvie_caris_xwalk/"))
folder1 <- c("controlfile", "datadictionary", "xwalk")
rwd360_xwalk <- list()
for (i in 1:length(folder1)) {
  print(i)
  setwd(file.path(getwd(), folder1[i]))
  files <- list.files(path = ".", full.names = T, recursive = T)
  s <- parquet(files)
  rwd360_xwalk[[i]] <- s
  setwd(file.path(proj_dir, "c3_rwd360_202312_abbvie_caris_xwalk/"))
}
names(rwd360_xwalk) <- folder1

######## PART 2: RWD360 EHR
setwd(file.path(proj_dir, "c3_rwd360_202312_abbvie_caris_all/"))
folder2 <- c("biomarker", "care_goal","condition", "controlfile",
             "datadictionary", "drug", "encounter", "her2_status_mo", "hr_tnbc_dx_date_mo",
             "line_of_therapy",
             "line_of_therapy_claims", "medication", "patient","patient_status_mo",
             "patient_test", "radiation","staging", "surgery", "tumor_exam")
rwd360_ehr <- list()
for (i in 1:length(folder2)) {
  print(i)
  setwd(file.path(getwd(), folder2[i]))
  files <- list.files(path = ".", full.names = T, recursive = T)
  s <- parquet(files)
  rwd360_ehr[[i]] <- s
  setwd(file.path(proj_dir, "c3_rwd360_202312_abbvie_caris_all/"))
}
names(rwd360_ehr) <- folder2


######## PART 3: RWD360 Open Claim
setwd(file.path(proj_dir, "c3_rwd360_open_202312_abbvie_caris_all/"))
folder3 <- c("condition", "controlfile", "datadictionary", "encounter", "imaging",
             "medication", "patient", "patient_sdoh",
             "radiation", "surgery")
rwd360_oc <- list()
for (i in 1:length(folder3)) {
  print(i)
  setwd(file.path(getwd(), folder3[i]))
  files <- list.files(path = ".", full.names = T, recursive = T)
  s <- parquet(files)
  rwd360_oc[[i]] <- s
  setwd(file.path(proj_dir, "c3_rwd360_open_202312_abbvie_caris_all/"))
}
names(rwd360_oc) <- folder3

######## PART 4: RWD360 Caris
setwd(file.path(proj_dir, "case_details_rwd360_202312_all/"))
folder4 <- c("case", "controlfile", "datadictionary","diagnosis",
             "patient","sequencing", 
             "sequencing_dna_copynumberalteration",
             "sequencing_dna_variant", "sequencing_gss",
             "sequencing_hla", "sequencing_hrd",
             "sequencing_loh",
             "sequencing_msi", "sequencing_rna_fusion","sequencing_rna_variant",
             "sequencing_tmb", "specimen", "stain_he","stain_ihc", "stain_ish",
             "test")
rwd360_caris <- list()
for (i in 1:length(folder4)) {
  print(i)
  setwd(file.path(getwd(), folder4[i]))
  files <- list.files(path = ".", full.names = T, recursive = T)
  s <- parquet(files)
  rwd360_caris[[i]] <- s
  setwd(file.path(proj_dir, "case_details_rwd360_202312_all/"))
}
names(rwd360_caris) <- folder4

## New function to extract # rows and # unique patients
extract_rows_pts <- function(input_list, input_list_name) {
  num.rows <- NULL
  num.unqpt <- NULL
  bucket <- NULL
  table <- NULL
  for (i in 1:length(input_list)) {
    num.rows <- c(num.rows, nrow(input_list[[i]]))
    if (sum(input_list_name %in% c("rwd360_EHR", "rwd360_OC")) >0) {
      num.unqpt <- c(num.unqpt, 
                     length(unique(input_list[[i]]$chai_patient_id)))
    } else {
      num.unqpt <- c(num.unqpt, 
                     length(unique(input_list[[i]]$patient_id)))
    }
    bucket <- c(bucket, input_list_name)
    table <- c(table, names(input_list)[i])
  }
  output <- data.frame(bucket, table, num.rows, num.unqpt)
  return(output)
}
rwd_ehr_controlfile <- extract_rows_pts(rwd360_ehr, "rwd360_EHR")
rwd_oc_controlfile <- extract_rows_pts(rwd360_oc, "rwd360_OC")
rwd_caris_controlfile <- extract_rows_pts(rwd360_caris, "rwd360_Caris")
# combine:
rwd_controlfile_all <- rbind(rwd_ehr_controlfile, rwd_oc_controlfile, rwd_caris_controlfile)
write.csv(rwd_controlfile_all,
          "/Users/wusx12/Library/CloudStorage/OneDrive-AbbVieInc(O365)/Documents/Projects/RWD/Caris_2024/03_data/Q1_2024/parquet/summary/Q12024_rwd360_controlfile.csv")


