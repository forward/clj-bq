# Clojure BigQuery wrapper

    (use 'clj-bq.core)
     
    ;; Check Google's API console for the credentials
    (def *credentials* { :client-id "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
                         :client-secret "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
                         :scope "https://www.googleapis.com/auth/bigquery"
                         :user-agent "clj-bq test client" })
     
    ;; OAuth dance
    (start-authentication credentials)
     
    ;; Follow the URL printed in stdout and authorize the client
    (def cred "browser introduced credential")
     
    ;; Finish OAuth
    (continue-authentication cred)
     
     
    ;; Big Query client
    (def bq (big-query))
     
     
    ;; list of projects
    (projects bq)
     
    ;; datasets for a project
    (datasets bq "Test Project")
     
    ;; creates an asynchronous job
    (async-job bq "Test Project" 
                  "SELECT word, word_count FROM publicdata:samples.shakespeare LIMIT 10")
     
    ;; Check the status of the jobs
    (jobs bq "Test Project")
     
    ;; Retrieve results
    (def job (first (jobs bq "Test Project")))
    (map as-tuples
         (take 20 (job-results bq "API Project" (:id job))))
