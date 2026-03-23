-- Run this code to create table for the dataset attached in the data file.

CREATE EXTERNAL TABLE IF NOT EXISTS clinic_data (
    appointment_id INT,
    patient_id INT,
    age INT,
    gender STRING,
    postcode_area STRING,
    distance_to_clinic_km DOUBLE,
    clinic_id STRING,
    clinic_type STRING,
    appointment_type STRING,
    consultation_fee_gbp INT,
    scheduled_date STRING,
    appointment_date STRING,
    appointment_day_of_week STRING,
    appointment_hour INT,
    days_in_advance INT,
    is_weekend INT,
    is_peak_hour INT,
    patient_total_appointments INT,
    patient_previous_noshow_count INT,
    patient_noshow_rate DOUBLE,
    days_since_last_visit INT,
    has_chronic_condition_flag INT,
    sms_reminder_sent INT,
    reminder_hours_before INT,
    reminder_response STRING,
    number_of_reminders_sent INT,
    estimated_revenue_loss_gbp DOUBLE,
    staff_idle_cost_gbp DOUBLE,
    slot_rebooked_flag INT,
    no_show INT,
    lead_time_days INT
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' 
STORED AS TEXTFILE
LOCATION 's3://your-folder-name/processed-data/'
TBLPROPERTIES (
    'skip.header.line.count'='1'
);