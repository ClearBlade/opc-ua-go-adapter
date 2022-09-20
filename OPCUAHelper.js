/**
 * Type: Library
 * Description: A library that contains a function which, when called, returns an object with a public API.
 */
function OPCUAHelper(browseResponse) {
  browseResponse.nodes.sort(compare);
  var _nodeTree = null;
  _populateNodeTree(browseResponse);

  function SetBrowseResponse(newBrowseResponse) {
    _populateNodeTree(newBrowseResponse);
  }

  function GetNodeIDFromPathsAndNodeNames(pathsAndNodeNames) {
    nodesToReturn = [];
    pathsAndNodeNames.forEach(function (pathAndNodeName) {
      var pathArray = pathAndNodeName.path.split(".");
      var localNodeTree = _nodeTree;
      for (var i = 0; i < pathArray.length; i++) {
        var childArray = [];

        localNodeTree = localNodeTree.filter(function (node) {
          return node.node_id === pathArray[i];
        });

        for (var j = 0; j < localNodeTree.length; j++) {
          childArray.push.apply(childArray, localNodeTree[j].children);
        }
        if (childArray.length > 0) {
          localNodeTree = childArray;
        }
      }

      var nodeToReturn = localNodeTree.find(function (node) {
        return node.browse_name === pathAndNodeName.node_name;
      });

      if (!!nodeToReturn) {
        nodesToReturn.push(nodeToReturn.node_id);
      }
    });
    return nodesToReturn;
  }

  function _populateNodeTree(browseResponse) {
    var map = {},
      node,
      roots = [],
      i;

    for (i = 0; i < browseResponse.nodes.length; i += 1) {
      map[browseResponse.nodes[i].node_id] = i; // initialize the map
      browseResponse.nodes[i].children = []; // initialize the children
    }

    for (i = 0; i < browseResponse.nodes.length; i += 1) {
      node = browseResponse.nodes[i];
      if (node.parent_node_id !== null && node.parent_node_id !== "") {
        browseResponse.nodes[map[node.parent_node_id]].children.push(node);
      } else {
        roots.push(node);
      }
    }
    _nodeTree = roots;
  }

  function compare(a, b) {
    if (a.level < b.level) {
      return -1;
    }
    if (a.level > b.level) {
      return 1;
    }
    return 0;
  }

  return {
    SetBrowseResponse,
    GetNodeIDFromPathsAndNodeNames,
  };
}
//find() polyfill
Array.prototype.find =
  Array.prototype.find ||
  function (callback) {
    if (this === null) {
      throw new TypeError("Array.prototype.find called on null or undefined");
    } else if (typeof callback !== "function") {
      throw new TypeError("callback must be a function");
    }
    var list = Object(this);
    // Makes sures is always has an positive integer as length.
    var length = list.length >>> 0;
    var thisArg = arguments[1];
    for (var i = 0; i < length; i++) {
      var element = list[i];
      if (callback.call(thisArg, element, i, list)) {
        return element;
      }
    }
  };
