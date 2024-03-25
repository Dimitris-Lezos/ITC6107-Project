# ITC6107-Project
This is an initial implementation of the ITC6107 Winter 2024 Project
All provided code needs more work especially <b>mine.py</b>
<h1>How to run the system</h1>
<h2>Start MongoDB</h2>
<li>As described in the slides provided by the Professor</li>
<li>In my local environment I use MongoDB on <b>Docker</b>:</li>
<b>Installation:</b> docker pull mongodb/mongodb-community-server:6.0-ubuntu2204
<br/>
<b>Run:</b> docker run --name mongodb -d -p 27017:27017 mongodb/mongodb-community-server:6.0-ubuntu2204
<h3>Create the "itc6107" database</h3>
<li><b>Start the mongo CLI:</b> mongosh mongodb://localhost:27017</li>
<li><b>Create the <u>itc6107</u> database:</b> use itc6107</li>
<li><b>Create/List the <u>blocks</u> collection:</b> db.blocks.find()</li>

<h2>Start Kafka</h2>
<li>As described in the slides provided by the Professor</li>
<li>In my local environment I use Kafka on <b>Docker</b>:</li>
<b>Installation:</b> docker pull apache/kafka:3.7.0
<br/>
<b>Run:</b> docker run --name kafka -p 9092:9092 apache/kafka:3.7.0
<h3>Create the "Blocks" topic</h3>
<b>Check if exists:</b> bin/kafka-topics.sh --topic Blocks --describe --bootstrap-server localhost:9092
<br/>
<b>Create if needed:</b> bin/kafka-topics.sh --create --topic Blocks --partitions 2 --bootstrap-server localhost:9092
<h2>Start the Python components</h2>
Each in a different <b>terminal</b>, or through <b>PyCharm</b>
<li>python tr-server.py</li>
<li>python mine.py</li>
<li>python app0.py</li>
<li>python app1.py</li>
<li>python mongoq.py</li>
