# Reactive Graph Demo

A simple example of a reactive graph that updates in real-time.

## Specification

- Use zmq for messaging
- Use protobuf for serialization
- Deploy zmq services as nodes which can both send and receive messages
- Deploy a fastapi dashboard app as a node which can subscribe to all the nodes using zmq and display the messaging activity on a dashboard in real time.
- It should be possible to control and tunes some of the nodes activity from the dashboard, to demonstrate that the graph is reactive and can be controlled from the dashboard.
- The demo should be around live streaming data from a stock exchange, where there are nodes that are responsible for:
    - Receiving and validating the data from the stock exchange
    - Processing the data and generating analytics
    - Storing the data in a database
    - Displaying the data on a dashboard
- Make sure the services and the dashboard can be deployed on datatailr and work as expected.

The example in pong.py is tested to be working as a deployed service on datatailr. When the script test_pong.py is run, it should be able to send a message to the pong service and receive a reply.