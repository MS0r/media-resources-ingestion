//! Job status types — single canonical `JobStatus` enum.
//!
//! The simple `JobStatus` enum is used for Redis (string) and filtering.
//! The rich `JobStatusDetail` struct is used for MongoDB and runtime.

use std::str::FromStr;

use serde::{Deserialize, Serialize};

/// Simple job status — no data in variants.
/// Used in Redis (`jobs:state:{id}.status`) and for filtering.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum JobStatus {
    Pending,
    Running,
    Retrying,
    Completed,
    Failed,
    Cancelled,
}

impl JobStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Retrying => "retrying",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }
}

impl FromStr for JobStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "pending" => Ok(Self::Pending),
            "running" => Ok(Self::Running),
            "retrying" => Ok(Self::Retrying),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            "cancelled" => Ok(Self::Cancelled),
            other => Err(format!("Unknown job status: '{other}'")),
        }
    }
}

impl std::fmt::Display for JobStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Filter for `list_jobs` — a newtype around `JobStatus`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JobStatusFilter(pub JobStatus);

impl JobStatusFilter {
    pub fn as_str(self) -> &'static str {
        self.0.as_str()
    }

    /// Returns the MongoDB field path for this status filter.
    /// Used with `doc! { field: { "$exists": true } }` to filter by status.
    pub fn mongo_field(self) -> &'static str {
        match self.0 {
            JobStatus::Pending => "status.pending",
            JobStatus::Running => "status.running",
            JobStatus::Retrying => "status.retrying",
            JobStatus::Completed => "status.completed",
            JobStatus::Failed => "status.failed",
            JobStatus::Cancelled => "status.cancelled",
        }
    }
}

impl FromStr for JobStatusFilter {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(JobStatus::from_str(s)?))
    }
}

impl Serialize for JobStatusFilter {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for JobStatusFilter {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        Self::from_str(&s).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_job_status_as_str() {
        assert_eq!(JobStatus::Pending.as_str(), "pending");
        assert_eq!(JobStatus::Running.as_str(), "running");
        assert_eq!(JobStatus::Retrying.as_str(), "retrying");
        assert_eq!(JobStatus::Completed.as_str(), "completed");
        assert_eq!(JobStatus::Failed.as_str(), "failed");
        assert_eq!(JobStatus::Cancelled.as_str(), "cancelled");
    }

    #[test]
    fn test_job_status_from_str() {
        assert_eq!(JobStatus::from_str("pending").unwrap(), JobStatus::Pending);
        assert_eq!(JobStatus::from_str("RUNNING").unwrap(), JobStatus::Running);
        assert_eq!(
            JobStatus::from_str("Completed").unwrap(),
            JobStatus::Completed
        );
        assert!(JobStatus::from_str("unknown").is_err());
    }

    #[test]
    fn test_job_status_display() {
        assert_eq!(JobStatus::Pending.to_string(), "pending");
        assert_eq!(JobStatus::Failed.to_string(), "failed");
    }

    #[test]
    fn test_job_status_serde_roundtrip() {
        for status in [
            JobStatus::Pending,
            JobStatus::Running,
            JobStatus::Retrying,
            JobStatus::Completed,
            JobStatus::Failed,
            JobStatus::Cancelled,
        ] {
            let json = serde_json::to_string(&status).unwrap();
            let deser: JobStatus = serde_json::from_str(&json).unwrap();
            assert_eq!(deser, status);
        }
    }

    #[test]
    fn test_job_status_filter_from_str() {
        assert_eq!(
            JobStatusFilter::from_str("pending").unwrap(),
            JobStatusFilter(JobStatus::Pending)
        );
        assert_eq!(
            JobStatusFilter::from_str("running").unwrap(),
            JobStatusFilter(JobStatus::Running)
        );
        assert!(JobStatusFilter::from_str("unknown").is_err());
    }

    #[test]
    fn test_job_status_filter_serde_roundtrip() {
        for status in [
            JobStatus::Pending,
            JobStatus::Running,
            JobStatus::Completed,
            JobStatus::Failed,
            JobStatus::Retrying,
            JobStatus::Cancelled,
        ] {
            let filter = JobStatusFilter(status);
            let json = serde_json::to_string(&filter).unwrap();
            let deser: JobStatusFilter = serde_json::from_str(&json).unwrap();
            assert_eq!(deser.0, status);
        }
    }
}
