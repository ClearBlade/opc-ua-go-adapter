/**
 * Helper stream service for filtering responses based on path and browse name
 */
function opcuaAdapterTest(req, resp) {
  ClearBlade.init({ request: req });
  var messaging = ClearBlade.Messaging();
  const OPCUA_BROWSE_RESPONSE_TOPIC = "opcua/browse/response/_platform";
  messaging.subscribe(OPCUA_BROWSE_RESPONSE_TOPIC, WaitLoop);

  function WaitLoop(err, data) {
    if (err) {
      log("error", "Subscribe failed: " + data);
      resp.error("Subscribe failed: ", data);
    } else {
      log("success", "Subscribed to Shared Topic. Starting Loop.");
      while (true) {
        messaging.waitForMessage(
          [OPCUA_BROWSE_RESPONSE_TOPIC],
          function (err, msg, topic) {
            if (err) {
              log(
                "error",
                "Failed to wait for message: " + err + " " + msg + "  " + topic
              );
              resp.error("Failed to wait for message: ", err);
            } else {
              log("Recieved message: " + msg + "  " + topic);
              if (
                JSON.parse(msg).connection_status.status === "BrowseSuccess"
              ) {
                msg = JSON.parse(msg);

                //get an array of node names
                var names = msg.node_list.map(function (node) {
                  return node.node_name;
                });

                //get an array of paths and get the final path part
                var paths = msg.node_list.map(function (node) {
                  var finalPartOfPath = node.path.substring(
                    node.path.lastIndexOf("."),
                    node.path.length
                  );
                  return finalPartOfPath;
                });

                //filter response by BrowseNames and paths, found in msg
                var filteredByBrowseNames = msg.nodes.filter(function (node) {
                  return names.includes(node.browse_name);
                });
                var filteredByBrowseNamesAndPaths =
                  filteredByBrowseNames.filter(function (node) {
                    return paths.includes(node.node_id);
                  });

                var responseMsg = {
                  nodes: filteredByBrowseNamesAndPaths,
                  connection_status: {
                    status: msg.connection_status.status,
                    timestamp: msg.connection_status.timestamp,
                  },
                };

                //publish the response
                messaging.publish(
                  "opcua/browse/response/filtered",
                  responseMsg
                );
              }
            }
          }
        );
      }
    }
  }
}
