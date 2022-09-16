/**
 * Type: Library
 * Description: A library that contains a function which, when called, returns an object with a public API.
 */
function OPCUAHelper(browseResponse) {
  browseResponse = JSON.parse(browseResponse);
  browseResponse.nodes.sort(compare);
  var _nodeTree = null;
  _populateNodeTree(browseResponse);

  function SetBrowseResponse(newBrowseResponse) {
    _populateNodeTree(newBrowseResponse);
  }

  function GetNodeIDFromPath(path, displayName) {
    var pathArray = path.split(".");
    var localNodeTree = _nodeTree;
    for (var i = 0; i < pathArray.length; i++) {
      localNodeTree = getNodes(localNodeTree, function (o) {
        return o.path.indexOf(pathArray[i]) !== -1;
      });
    }

    return localNodeTree.find(function (node) {
      return node.display_name === displayName;
    });
  }

  function getNodes(array, cb) {
    return array.reduce(function iter(r, a) {
      var children;
      if (cb(a)) {
        return r.concat(a);
      }
      if (Array.isArray(a.children)) {
        children = a.children.reduce(iter, []);
      }
      if (children.length) {
        return r.concat({ children: children });
      }
      return r;
    }, []);
  }

  function _populateNodeTree(browseResponse) {
    var map = {},
      node,
      roots = [],
      i;

    for (i = 0; i < browseResponse.nodes.length; i += 1) {
      map[browseResponse.nodes[i].id] = i; // initialize the map
      browseResponse.nodes[i].children = []; // initialize the children
    }

    for (i = 0; i < browseResponse.nodes.length; i += 1) {
      node = browseResponse.nodes[i];
      if (node.parent_node_id !== null) {
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
    GetNodeIDFromPath,
  };
}
