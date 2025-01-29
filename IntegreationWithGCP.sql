--IntefreationWithGCP
-- Create integreation obj that contains the access information 
CREATE STORAGE INTEGRATION gcp_integration
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = GCS
    ENABLED = TRUE
    STORAGE_ALLOWED_LOCATIONS = ('gcs://snowflackbucketgcps','gcs://snowflakebucketgcpjsons');

-- Describe integration object to provide access
DESC STORAGE INTEGRATION gcp_integration