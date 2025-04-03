#[derive(Debug, Clone, PartialEq)]
pub struct AttendanceLog {
    pub log_id: i32,
    pub employee_id: String,
    pub log_dtime: String,
    pub add_by: i32,
    pub add_dtime: String,
    pub insert_dtr_log_pic: Option<String>,
    pub hr_approval: Option<String>,
    pub dtr_type: Option<String>,
    pub remarks: Option<String>,
}