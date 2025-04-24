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

#[derive(Debug, Clone, PartialEq)]
pub struct SchedulingRecord {
    pub scheduling_id: i32,
    pub date_start: String,
    pub date_end: String,
    pub remarks: Option<String>,
    pub station: Option<String>,
    pub employee_id: String,
    pub department: String,
    pub time_start: String,
    pub time_end: String,
    pub updated_at: String,
    pub created_at: String,
    pub case: Option<String>,
    pub remark_2nd: Option<String>,
    pub display_order: Option<String>,
    pub display_order_2nd: Option<String>,
}