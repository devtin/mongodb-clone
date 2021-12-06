# mongodb-clone

This module stream's documents from the given collection source and pipes them to the collection destination:


## Usage

Create a config file to store src and destination configuration:
```js
// mongodb-clone.config.js

module.exports = {
  src: {
    credentials: 'mongodb://user:password@src-host:27017',
    database: 'src_database',
    collection: 'src_collection',
    query: {} // retrieve all documents
  },
  dst: {
    credentials: 'mongodb://user:password@dst-host:27017',
    database: 'dst_database',
    collection: `dst_collection_clone_${new Date().toLocaleDateString("en-US").replace(/\//g, '_')}`
  },
  flow: 1000 // amount of concurrent documents processed at once; avoids back-pressure
}
```

Then go ahead and:

```bash
npx mongodb-clone
```

<img src="./mongodb-clone.gif" width="800">

* * *

## License

[MIT](https://opensource.org/licenses/MIT)

&copy; 2021 Martin Rafael <tin@devtin.io>
