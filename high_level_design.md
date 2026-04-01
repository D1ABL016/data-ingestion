# Processing Flow

- The client will call an endpoint to upload a file to our server synchronously.
- After upload, the client will receive a polling API to check the processing status.
- Once the file is completely uploaded, a background task is triggered.
- The file will be split into multiple chunks, with each chunk having a maximum of 1000 rows.
- Status will be updated after the completion of each task.

---

# Chunk Processing

## At the start of each chunk:
- Required data from lookup tables will be fetched and stored in memory.
- If a record already exists, its ID will be reused.
- Otherwise, bulk insertion will be used to insert new records.

## Processing behavior:
- Each chunk will be processed sequentially.

## After processing a chunk:
- The processing status will be updated in the database.
- This includes:
  - Number of processed rows
  - Number of successful rows
  - Number of failed rows (with errors)

- The next chunk will then be processed.
- This cycle continues until the entire file is processed.

## After completion:
- The file will be deleted from the server to reduce storage load.

---

# Validation Strategy

- Only CSV format is allowed.
- Data validation will be handled using Pydantic models.
- Headers must be strictly validated. If not, an error will be thrown.

## Additional business validation:
- A user cannot be their own supervisor.
- Such records will be skipped.
- store_id must start with "STR" , if not then skip it.
- external_store_id must start with "EXT" , if not then skip it.

---

# Performance Strategy

- Files will be divided into chunks of 1000 rows.
- Bulk insert operations will be used to reduce database load.

## Example:
- For a file with 500,000 rows:
  - Total chunks = 500,000 / 1,000 = 500 chunks
  - Progress will be continuously updated in the database for the user

---

# Failure Handling

- If a row fails validation:
  - It will be marked as failed and stored with error details.
  - Valid rows will still be inserted into the database.

- The entire file will not be rejected due to individual row failures.
- A file with minor issues (e.g., 1 invalid row out of 500) should not be skipped entirely.

---

# Trade-offs Considered

- There is no access to S3 in the current setup.

## With S3, the following improvements could be made:
- Use multipart upload for better file handling.
- Introduce a separate microservice (e.g., Celery worker) for asynchronous processing.

## Benefits of this approach:
- Improved performance
- Better scalability
- Reduced peak load on the main server