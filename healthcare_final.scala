// Databricks notebook source
// MAGIC %sql
// MAGIC select hgi.*,hrrph.measure_name, hrrph.number_of_discharges, hrrph.footnote, hrrph.predicted_readmission_rate, hrrph.expected_readmission_rate, hrrph.number_of_readmissions, hvbp.fiscal_year, hvbp.discharge_information_floor, hvbp.communication_about_medicines_benchmark, hvbp.communication_about_medicines_floor, 
// MAGIC hvbp.care_transition_benchmark, hvbp.care_transition_baseline_rate, hvbp.communication_with_nurses_floor, hvbp.communication_with_doctors_floor from default.hospital_general_information hgi left join default.hospital_readmissions_reduction_program_hospital hrrph 
// MAGIC on hgi.facility_id = hrrph.facility_id and hgi.facility_name = hrrph.facility_name and 
// MAGIC hgi.state = hrrph.state left join
// MAGIC default.hvbp_person_and_community_engagement hvbp on 
// MAGIC hgi.facility_id = hvbp.facility_id and hgi.facility_name = hvbp.facility_name and 
// MAGIC hgi.state = hvbp.state and hgi.city = hvbp.city and hgi.zip_code = hvbp.zip_code and hgi.county_name = hvbp.county_name where hvbp.fiscal_year = '2022';

// COMMAND ----------

val hgi = spark.sql("select * from default.hospital_general_information").alias("hgi")
val hrrph = spark.sql("select * from default.hospital_readmissions_reduction_program_hospital").alias("hrrph")
val hvbp = spark.sql("select * from default.hvbp_person_and_community_engagement").alias("hvbp")

val healthcare_final = hgi.alias("hgi").join(hrrph.alias("hrrph"), $"hgi.facility_id" === $"hrrph.facility_id" && $"hgi.facility_name" === $"hrrph.facility_name" && $"hgi.state" === $"hrrph.state", "left").join(hvbp.alias("hvbp"), $"hgi.facility_id" === $"hvbp.facility_id" && $"hgi.facility_name" === $"hvbp.facility_name" && $"hgi.state" === $"hvbp.state" && $"hgi.city" === $"hvbp.city" && $"hgi.zip_code" === $"hvbp.zip_code" && $"hgi.county_name" === $"hvbp.county_name","left").selectExpr("hgi.*","hrrph.measure_name","hrrph.number_of_discharges","hrrph.footnote","hrrph.predicted_readmission_rate","hrrph.expected_readmission_rate","hrrph.number_of_readmissions","hvbp.fiscal_year"," hvbp.discharge_information_floor","hvbp.communication_about_medicines_benchmark","hvbp.communication_about_medicines_floor","hvbp.care_transition_benchmark","hvbp.care_transition_baseline_rate","hvbp.communication_with_nurses_floor","hvbp.communication_with_doctors_floor").filter($"hvbp.fiscal_year" === "2022")

// healthcare_final.write.mode("overwrite").insertInto("default.healthcare_final")

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from default.healthcare_final
