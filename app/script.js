document.getElementById('searchForm').addEventListener('submit', function(event) {
    event.preventDefault();
    var searchTerm = document.getElementById('searchInput').value;
    // trim the search term 
    searchTerm = searchTerm.trim();
    var message =[searchTerm];
    var serializedTerm =  serialize({message:message});
    document.getElementById('resultList').textContent = 'Search for: ' + searchTerm;
      fetch('http://localhost:7070/query/get', {
        method:"PUT",
        headers: {
          'Content-Type': 'application/json',
        },
        // as response.on is data, maybe data:, format of this
        body:serializedTerm,
    }).then(response => {
      
      if (!response.ok) {
          throw new Error('Network response was not ok');
      }
      return response.json(); // Parse the JSON in the response
  })
    .then(data => {
        queryResult = deserialize(JSON.stringify(data))[1][0][searchTerm]

        document.getElementById('resultList').innerHTML = processQueryResult(queryResult);
    });
  //   axios({
  //     method: 'put',
  //     url: 'http://localhost:7070/query/get',
  //     headers: { 'Content-Type': 'application/json' },
  //     data: serializedTerm
  // })
  // .then(function(response) {
  //     console.log(response.data.value[1]); // Handle the response data
  //     var deserializedData = deserialize(JSON.stringify(response.data.value[1]));
  //     console.log(deserializedData[searchTerm]);
  //     document.getElementById('resultList').textContent = deserializedData[searchTerm];
  // })
  // .catch(function(error) {
  //     console.error('Error:', error);
  // });

  
});
function processQueryResult(queryResult){
  var htmlList = [];
  for(let i = 0; i < queryResult.length - 1; i += 1){
    input = queryResult[i];
    input = input.trim();
    if (input.startsWith('|')) {
      input = input.substring(1);
    }
    input = input.trim();
    input= input.split(/\s+/).filter(Boolean).join(' ');
    // Splitting the input string by spaces to isolate URLs and their corresponding ranks
    const parts = input.split(' ');
    const linkMap = {};

    // Processing parts in pairs to map URLs to their aggregated ranks
    for (let i = 0; i < parts.length - 1; i += 2) {
        const url = parts[i];
        const rank = parseInt(parts[i + 1], 10);

        if (linkMap[url]) {
            linkMap[url] += rank;
        } else {
            linkMap[url] = rank;
        }
    }

    // Sorting URLs based on the total aggregated rank
    const sortedLinks = Object.entries(linkMap)
        .sort((a, b) => b[1] - a[1])
        .map(([url, rank]) => `<li><a href="${url}">${url}</a></li>`);

    // Generating an HTML list of sorted URLs with their ranks
    htmlList = htmlList.concat(sortedLinks);
  }
  return htmlList.join('\n');
//   // Step 1: Parse the array and aggregate ranks
//   const linkMap = links.reduce((acc, item) => {
//     const parts = item.trim().split(' ');
//     const rank = parseInt(parts.pop(), 10);
//     const url = parts.join(' ').trim().substring(2);
//     acc[url] = (acc[url] || 0) + rank;
//     return acc;
//   }, {});

// // Step 2: Sort the links by their aggregated ranks
// const sortedLinks = Object.entries(linkMap)
//   .sort((a, b) => b[1] - a[1])
//   .map(([url, rank]) => ({ url, rank }));

// // Step 3: Generate HTML output
// const listItems = sortedLinks.map(link => `<li>${link.url} (Rank: ${link.rank})</li>`).join('\n');
// return listItems
}
// Class for the smallest unit of serialization
function Node(value, type, id=-1) {
    this.value = value; // value of the node (could be nested)
    this.type = type; // type of the value (for deserialization)
    if (id != -1) {
      this.id = id; // only ref type objcets will have id (object, array...)
    }
  }
  
  Node.prototype.addValue = function(key, value) {
    if (this.type === TYPES.array) {
      this.value.push(value);
    } else {
      this.value[key] = value;
    }
  };
  
  /* Global Variables */
  const BASIC_TYPES = new Set(
      ['number', 'string', 'boolean', 'undefined', 'null'],
  );
  const ITERABLE_TYPES = new Set(
      ['object', 'array', 'date', 'error'], // date, error are deried from object
  );
  const TYPES = {
    'number': 'number',
    'string': 'string',
    'boolean': 'boolean',
    'undefined': 'undefined',
    'null': 'null',
    'object': 'object',
    'array': 'array',
    'date': 'date',
    'error': 'error',
    'function': 'function',
    'ref': 'ref',
    'native': 'native',
  };
  
  const UNDEFINED_NODE = new Node(undefined, TYPES.undefined); // singleton node
  const NULL_NODE = new Node(null, TYPES.null); // singleton node
  const NATIVE_TO_ID = constructNativeMap(); // native functions/objects to id
  
  // for (const key of globalThisKeysStart) {
  //   constructNativeMap(NATIVE_TO_ID, globalThis[key]);
  // }
  // console.log('globalThisKeysStart:', globalThisKeysStart.length);
  // console.log('globalThis keys:', Reflect.ownKeys(globalThis).length);
  // console.log('native to id:', NATIVE_TO_ID.size);
  
  const ID_TO_NATIVE = reverseMap(NATIVE_TO_ID); // id to native functions/objects
  
  /**
     * Construct a mapping for all native functions/objects to an id
     *
     * @param {Map} map - a map to memorize the result
     * @param {Object} obj - an object to traverse
     * @return {Map} A map that maps all native functions/objects to an id
     */
  function constructNativeMap(map=new Map(), obj=globalThis) {
    if (typeof obj != TYPES.object && typeof obj != TYPES.function) {
      return map;
    } else if (map.has(obj) || !obj) {
      return map;
    }
    map.set(obj, map.size);
    // console.log(obj.name);
    if (typeof obj === TYPES.object && obj) {
      // Traverse the subobject
      // for (const key in obj) {
      //   if (obj.hasOwnProperty(key)) {
      //     constructNativeMap(map, obj[key]);
      //   }
      // }
      Reflect.ownKeys(obj).forEach((key) => constructNativeMap(map, obj[key]));
    }
    return map;
  }
  
  /**
     * Reverse all (key, value) pairs in a map and return a new map
     *
     * @param {Map} map a map
     * @return {Map} the reversed map
     */
  function reverseMap(map) {
    const newMap = new Map();
    map.forEach((value, key) => newMap.set(value, key));
    return newMap;
  }
  
  /**
     * Helper function for getting the parameters and body of a function
     *
     * @param {Function} f a function
     * @return {List} [funtion parameter list, function body value]
     */
  function getFunctionParametersAndBody(f) {
    if (typeof f !== TYPES.function) {
      return [[], ''];
    }
  
    const fStr = f.toString();
    const paramMatch = fStr.match(/^function.*?\(([^)]*)\)/) ||
                         fStr.match(/^async\s*function.*?\(([^)]*)\)/) ||
                         fStr.match(/^\(([^)]*)\)\s*=>/) ||
                         fStr.match(/^async\s*\(([^)]*)\)\s*=>/);
  
    const params = (!paramMatch) ? [] : paramMatch[1]
        .split(',')
        .map((param) => param.trim())
        .filter((param) => param !== '');
  
    const bodyMatch = fStr.match(/\{([\s\S]*)\}/);
    const body = bodyMatch && bodyMatch[1] ?
                      bodyMatch[1].trim() :
                      'return ' + fStr.split(' => ')[1];
  
    // return [params, body.replace(f.name, 'arguments.callee')];
    return [params, body];
  }
  
  /**
     * Serialize an object to a json string
     *
     * @param {Object} object - Object to be serialized
     * @return {String} The result json string of the serialized object.
     */
  function serialize(object) {
    const root = serializeHelper(object, new Map());
    return JSON.stringify(root);
  }
  
  /**
     * Helper function to convert all types of values to Node for later stringify
     *
     * @param {Object} object Object to be converted to Node
     * @param {Map} objToRefId A map that maps an object to its refered node id
     * @return {Node} Root node of the object
     */
  function serializeHelper(object, objToRefId) {
    // If this object has already been processed
    if (NATIVE_TO_ID.has(object)) {
      return new Node(NATIVE_TO_ID.get(object), TYPES.native);
    } else if (objToRefId.has(object)) {
      return new Node(objToRefId.get(object), TYPES.ref);
    }
  
    let currType = (object === null) ? TYPES.null : typeof object;
    const id = objToRefId.size;
  
    if (BASIC_TYPES.has(currType)) {
      if (currType === TYPES.undefined) {
        return UNDEFINED_NODE;
      } else if (currType === TYPES.null) {
        return NULL_NODE;
      }
      return new Node(object, currType);
    } else if (ITERABLE_TYPES.has(currType)) {
      objToRefId.set(object, id);
  
      if (object instanceof Date) {
        return new Node(object.getTime(), TYPES.date, id);
      } else if (object instanceof Error) {
        const data = {'message': object.message, 'stack': object.stack};
        const root = new Node(data, TYPES.error, id);
        return root;
      }
  
      // Handle the array or a regular object
      const isArray = Array.isArray(object);
      currType = isArray ? TYPES.array : currType;
      const root = new Node(isArray ? [] : {}, currType, id);
      for (const k in object) {
        if (object.hasOwnProperty(k) || !object.hasOwnProperty(k)) {
          const value = serializeHelper(object[k], objToRefId);
          if (value != null) {
            root.addValue(k, value);
          }
        }
      }
      return root;
    } else if (currType === TYPES.function) {
      // should save arguments and function bodies
      const root = new Node({}, currType);
      const [funcParameters, functionBody] = getFunctionParametersAndBody(object);
      if (functionBody === '') {
        return null;
      }
      root.value.parameters = funcParameters;
      root.value.body = functionBody;
      root.value.name = object.name;
      return root;
    }
  
    // For any other unrecognied type, do not serialize and return null
    return null;
  }
  
  /**
     * Deserialize an object
     * After JSON parsing, the object is a Node we defined
     *
     * @param {String} string JSON string needed to be deserialized
     * @return {Object} The original object
     */
  function deserialize(string) {
    const node = JSON.parse(string);
    return deserializeHelper(node, new Map(ID_TO_NATIVE));
  }
  
  /**
     * Helper function for deserialization
     *
     * @param {Node} root Root of the object to parse
     * @param {Map} idToObj A map that maps node id to its original object
     * @return {Object} The original object
     */
  function deserializeHelper(root, idToObj) {
    const currType = root.type;
  
    if (BASIC_TYPES.has(currType)) {
      return root.value;
    } else if (ITERABLE_TYPES.has(currType)) {
      if (currType === TYPES.date) {
        const res = new Date(root.value);
        idToObj.set(root.id, res);
        return res;
      } else if (currType === TYPES.error) {
        const res = new Error(root.value.message);
        res.stack = root.value.stack;
        idToObj.set(root.id, res);
        return res;
      }
  
      const res = root.value;
      idToObj.set(root.id, res);
      for (const e in res) {
        if (res.hasOwnProperty(e)) {
          res[e] = deserializeHelper(res[e], idToObj);
        }
      }
      return res;
    } else if (currType === TYPES.function) {
      if (root.value.body.includes('await')) {
        console.log(root.value.body);
      }
      const f = new Function(...root.value.parameters, root.value.body);
      Reflect.defineProperty(f, 'name', {value: root.value.name});
      return f;
    } else if (currType === TYPES.ref) {
      return idToObj.get(root.value);
    } else if (currType === TYPES.native) {
      return ID_TO_NATIVE.get(root.value);
    }
  
    return null;
  }
  
