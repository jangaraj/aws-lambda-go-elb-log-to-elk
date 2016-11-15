package main

import (
        "encoding/json"
        "log"
        "time"
        "bufio"
        "strings"
        "strconv"

        elastic "gopkg.in/olivere/elastic.v3"
        "github.com/eawsy/aws-lambda-go/service/lambda/runtime"
        "github.com/aws/aws-sdk-go/aws"
        "github.com/aws/aws-sdk-go/aws/session"
        "github.com/aws/aws-sdk-go/aws/credentials"
        "github.com/aws/aws-sdk-go/service/s3"

)

// Hardcoded config options - it's not good idea to store passwords/keys here
// More about config options https://www.concurrencylabs.com/blog/configure-your-lambda-function-like-a-champ-sail-smoothly/
// bulk_limit - 5k log lines ~ 3MB of data - we can index one access log file in one go
var Config = map[string]map[string]string{
    "LIVE": map[string]string{
        "debug":                 "false",
        "elk_url":               "http://<IP>:9200",
        "bulk_limit":            "6000",
        "region":                "eu-west-1",
    },
    "DEV": map[string]string{
        "debug":      "false",
        "elk_url":    "http://<IP>:9200",
        "bulk_limit": "6000",
        "region":     "eu-west-1",
    },
    "LOCAL": map[string]string{
        "debug":      "true",
        "elk_url":    "http://127.0.0.1:9200",
        "bulk_limit": "6000",
        "region":     "eu-west-1",
    },
}


// Object provides information about the S3 object which has been created or
// deleted.
type Object struct {
    // The object key.
    Key string `json:"key"`

    // The object size in bytes. Provided for "ObjectCreated" event, otherwise 0.
    Size int `json:"size"`

    // The object eTag. Provided for "ObjectCreated" event, otherwise empty
    // string.
    ETag string `json:"eTag"`

    //  The object version if bucket is versioning-enabled, otherwise empty
    // string.
    VersionID string `json:"versionId"`

    // A string representation of a hexadecimal value used to determine event
    // sequence, only used with PUTs and DELETEs.
    Sequencer string `json:"sequencer"`
}

// Bucket provides information about the S3 bucket the S3 object has been
// created or deleted.
type Bucket struct {
    // The bucket arn.
    ARN string `json:"arn"`

    // The bucket name.
    Name string `json:"name"`

    // The Amazon customer ID of the bucket owner.
    OwnerIdentity UserIdentity `json:"ownerIdentity"`
}

// Record is the unit of data of the S3 event
type Record struct {
    // The schema version for the record.
    SchemaVersion string `json:"s3SchemaVersion"`

    // The ID found in the bucket notification configuration.
    ConfigurationID string `json:"configurationId"`

    // The S3 bucket information
    Bucket *Bucket `json:"bucket"`

    // The S3 object information
    Object *Object `json:"object"`
}

// ResponseElements contains information about the Amazon S3 host.
type ResponseElements struct {
    // The Amazon S3 host that processed the request.
    AmzID2 string `json:"x-amz-id-2"`

    // The Amazon S3 generated request ID.
    AmzRequestID string `json:"x-amz-request-id"`
}

// RequestParameters contains information about the sender of the S3 request.
type RequestParameters struct {
    // The ip address where request came from.
    SourceIPAddress string `json:"sourceIPAddress"`
}

// UserIdentity is a container for an Amazon customer ID
type UserIdentity struct {
    // An Amazon customer ID
    PrincipalID string `json:"principalId"`
}

// EventRecord provide contextual data about a S3 event.
//
// For more information about the possible values of the EventName see Event
// Notification Types and Destinations section of Configuring Amazon S3 Event
// Notifications in Amazon Simple Storage Service Developer Guide.
//
// http://docs.aws.amazon.com/AmazonS3/latest/dev/NotificationHowTo.html#notification-how-to-event-types-and-destinations
type EventRecord struct {
    // The event version.
    EventVersion string `json:"eventVersion"`

    // The event Source. Always "aws.s3".
    EventSource string `json:"eventSource"`

    // The AWS region where the event originated.
    AwsRegion string `json:"awsRegion"`

    // The time, in ISO-8601 format, for example, 1970-01-01T00:00:00.000Z, when
    // S3 finished processing the request.
    EventTime time.Time `json:"eventTime"`

    // The S3 event name.
    EventName string `json:"eventName"`

    // The Amazon customer ID of the user who caused the event.
    UserIdentity UserIdentity `json:"userIdentity"`

    // Information about the sender of the S3 request
    RequestParameters RequestParameters `json:"requestParameters"`

    // Information about the S3 host.
    ResponseElements ResponseElements `json:"responseElements"`

    // The underlying S3 request associated with the event.
    S3 Record `json:"s3"`
}

// Event represents a S3 event.
//
// For more information about the event message see Event Message Structure in
// Amazon Simple Storage Service Developer Guide.
//
// http://docs.aws.amazon.com/AmazonS3/latest/dev/notification-content-structure.html
type S3Event struct {
    // The list of S3 event records.
    Records []*EventRecord `json:"Records"`
}

func handle(evt json.RawMessage, ctx *runtime.Context) (interface{}, error) {
        rt := ctx.RemainingTimeInMillis()
        // hardcoded config
        // more about config options https://www.concurrencylabs.com/blog/configure-your-lambda-function-like-a-champ-sail-smoothly/
        arns := strings.Split(ctx.InvokedFunctionARN, ":")
        env := arns[len(arns)-1]

        debug, _ := strconv.ParseBool(Config[env]["debug"])
        bulk_limit, _ := strconv.Atoi(Config[env]["bulk_limit"])

        if debug {
            log.Printf("Used configuration env: %s", env)
            log.Printf("Configuration: %s", Config[env])
            log.Printf("Received event: %s\n", string(evt))
            log.Printf("Context: %+v", ctx)
            log.Println("ELK index: ", "logstash-" + time.Now().UTC().Format("2006.01.02"))
        }

        // keep eyes on timeout
        select {
        case <-time.After(500 * time.Millisecond):
                rt = ctx.RemainingTimeInMillis()
                if rt < 1000 {
                   log.Printf("WARNING: Time left before timeout: %d", rt)
                }
        }

        var s3rs S3Event

        err := json.Unmarshal(evt, &s3rs)
        if err != nil {
            log.Fatal(err)
        }

        for _, er := range s3rs.Records {
            sess := session.New()
            region := Config[env]["region"]
            var svc *s3.S3
            if _, ok := Config[env]["aws_secret_access_key"]; ok {
                if debug {
                    log.Println("AWS auth with access key/secret")
                }
                creds := credentials.NewStaticCredentials(Config[env]["aws_access_key_id"], Config[env]["aws_secret_access_key"], "")
                svc = s3.New(sess, &aws.Config{
                    Region:      &region,
                    Credentials: creds,
                })
            } else {
                if debug {
                    log.Println("AWS auth with IAM role")
                }
                svc = s3.New(sess, &aws.Config{
                    Region:      &region,
                })
            }

            result, err := svc.GetObject(&s3.GetObjectInput{
                Bucket: aws.String(er.S3.Bucket.Name),
                Key:    aws.String(er.S3.Object.Key),
            })
            if err != nil {
                log.Fatal("Failed to get object: ", err)
            }

            client, err := elastic.NewClient(elastic.SetURL(Config[env]["elk_url"]))
            if err != nil {
               log.Fatal("elk client error: ", err)
            }

            bulkRequest := client.Bulk()

            // create a new scanner and read the file line by line
            num := 1
            scanner := bufio.NewScanner(result.Body)
            for scanner.Scan() {
                if debug {
                    log.Printf("Log line %d: %s", num, scanner.Text())
                }


                // Add a document to the index
                oline := scanner.Text()
                line := strings.Split(oline, " ")
                t, err :=  time.Parse(time.RFC3339, line[0])
                if err != nil {
                   log.Println("Error parsing log date: ", err)
                }
                temp := strings.Split( strings.Split(oline, "\" \"")[1], "\"")
                elog := map[string]string{}
                elog["@log_name"] = "elb_access_log"
                elog["@timestamp"] =  t.UTC().Format("2006-01-02T15:04:05-0700")
                elog["elb"] = line[1]
                elog["client"] = line[2]
                elog["backend"] = line[3]
                elog["requestprocessingtime"] = line[4]
                elog["backendprocessingtime"] = line[5]
                elog["responseprocessingtime"] = line[6]
                elog["elbstatuscode"] = line[7]
                elog["backendstatuscode"] = line[8]
                elog["receivedbytes"] =  line[9]
                elog["sentbytes"] =  line[10]
                elog["requesttype"] = strings.Trim(line[11], "\"")
                elog["requesturl"] =  line[12]
                elog["requestprotocol"] = strings.Trim(line[13], "\"")
                elog["useragent"] = temp[0]
                elog["sslcipher"] = strings.Split(temp[1], " ")[1]
                elog["sslprotocol"] = strings.Split(temp[1], " ")[2]

                if debug {
                    log.Printf("Inserting document into ELK: %+v\n", elog)
                }

                r := elastic.NewBulkIndexRequest().
                    Index("logstash-" + time.Now().UTC().Format("2006.01.02")).
                    Type("elblog").
                    Doc(elog)

                bulkRequest = bulkRequest.Add(r)
                actions :=  bulkRequest.NumberOfActions()
                if actions == bulk_limit {
                    if debug {
                        log.Println("ELK bulk indexing - document count: ",  actions)
                    }

                    bulkResponse, err := bulkRequest.Do()
                    if err != nil {
                         log.Println("ELK bulk indexing error: ", err)
                    }

                    created := bulkResponse.Created()
                    if len(created) != actions {
                        log.Println("Some documents haven't been created: ", actions - len(created))
                        for i := 0; i < len(created); i++ {
                            log.Printf("%d: %s %s\n", i, created[i].Error, created[i].Status)
                        }
                    }
                }
                num++
            }

            // index rest of logs
            actions :=  bulkRequest.NumberOfActions()
            if actions > 0 {
                if debug {
                    log.Println("Last ELK bulk indexing - document count: ",  actions)
                }

                bulkResponse, err := bulkRequest.Do()
                if err != nil {
                    log.Println("Last ELK bulk indexing error: ", err)
                }

                created := bulkResponse.Created()
                if len(created) != actions {
                    log.Println("Some documents haven't been created: ", actions - len(created))
                    for i := 0; i < len(created); i++ {
                        log.Printf("%d: %s %s\n", i, created[i].Error, created[i].Status)
                    }
                }
            }
             log.Println("Lines processed: ",  num)
        }
        return nil, nil
}

func init() {
        runtime.HandleFunc(handle)
}

func main() {}
