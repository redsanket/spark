;; ## smile-http-conf.clj
;;
;; This sample SMILE configuration uses HTTP interface for injecting training examples
;;

(import '[smile.storm.classification Avg HttpSource HDFSSource DRPCSource DRPCSink])
(import '[smile.storm.classification GradientDescent ])
(import '[smile.partitioning Broadcast Shuffle])

{
 :training_source {
                :type HttpSource
                :injection.uri "http://smile.test:4080/"
                :registry.uri "http://registry-a.red.ygrid.yahoo.com:4080/registry/v1/"
                :spout.parallelism 1
                :event.queue.size 20000
                   }
 :query_source {
                :type DRPCSource
                :func "gradientquery"
                }
 :query_sink {
                :type DRPCSink
              }

 :classifiers [{
                :type GradientDescent
                :registry.uri "http://registry-a.red.ygrid.yahoo.com:4080/registry/v1/"
                :refresh.uri "http://smile.test.refresh:8080/"
                :learner.parallelism 1
                :training_partitioner Shuffle
                :query_partitioner Broadcast
                :response_aggregator Avg
                :learning_rate 0.1
                :decay_rate 2
                :replicate true
                :replicate.frequency 5
                :model_storage.uri "file:///homes/mapredqa/test_models/"
                :model_storage.model_name "test-gd"
                :save.model.every 30000
                }]

 :name "smile_gradient_persist"

 :local false

}
