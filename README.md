# TightCommunityDetection
Detect Tight Communities in a social Network

Using Spark GraphX to form graph and find cliques in the graph to detect smaller stronger sub-graphs representing tight communties. 

Data Used : Facebook ( http://snap.stanford.edu/data/egonets-Facebook.html )

Execution : 

1) Download data and unzip jar into a folder ( upload folder to AWS S3 if running on EMR ) 
2) Import as a sbt project into IntelliJ IDE
3) Assembly build the project ( upload jar into S3 bucket )
4) Run in IntelliJ or in AWS EMR cluster 

Results : A file containing the node clusters representing tight communties 
