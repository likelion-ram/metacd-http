# METACD protocol

## Conventions

We talk about *URL prefix* and *URL options*. This concern the path of the URL. The options are concatened using '/'.

In the following description, a few ``${...}`` elements are presented. For each element, here are the constraints on the element's values:
  * ``${NS}`` : a known namespace, typically a string with the form of a domain name. The physical namespace is the main token when read from left to right. According to Redcurrant's standards, a namespace token should not contain any character that is not alphanumerical.
  * ``${TYPE}`` :  a service type, single (e.g. ``meta2``) or compound (e.g. ``sqlx.plop``). the physical service type is the forst token when read from left to right. According to Redcurrant's standards, a service type's  token should not contain any character that is not alphanumerical.
  * ``${REF}`` : 
  * ``${PATH}`` : 
  * ``${TYPE}`` : a service type
  * ``${INT}`` : an integer in decimal form.

## Conscience operations

### Configuration
  * URL ``/cs/info``
    * ``ns/${NS}``
  * **GET** Gets a namespace\_info in its JSON form in the body of the reply.
  * **HEAD** Check the namespace is known

### Services management
  * URL ``/cs/srv`` : play on collection on services. What identifies a collection is the NS it belongs to and its type name.
    * ``ns/${NS}``
    * ``type/${TYPE}``
  * **PUT** registers a list of services in the given collection
    * input : a JSON encoded array of services. The given score will be ignored.
  * **GET** get the list of services in the collection.
  * **HEAD** Check the service type is known for this namespace
  * **DELETE** flush a service definition or a single service
  * **POST**
    * ``action/lock`` force the score of the service described in the body [to the value in the description].
    * ``action/unlock`` releases the lock on the service score. The tags are ignored.

## High-level Directory operations

### Reference management
  * URL ``/dir/ref``
    * ``ns/${NS}``
  * **PUT** Reference creation
    * ``ref/${REF}``
  * **DELETE** Reference destruction
    * ``cid/${CID}`` or ``ref/${REF}``
  * **HEAD** Reference presence check
    * ``cid/${CID}`` or ``ref/${REF}``
  * **GET** Reference presence check
    * ``cid/${CID}`` or ``ref/${REF}``

### Services management
  * URL ``/dir/srv``
    * ``ns/${NS}``
    * ``ref/${REF}`` or ``cid/${CID}``
    * ``type/${TYPE}``
  * **GET** List the associated services
  * **DELETE** Removes an associated service.
  * **POST** Ensure a valid service is associated
    * ``action/link`` 
    * ``action/renew`` 
    * ``action/force`` 
    * input body : JSON encoded service description

### Properties handling
  * URL ``/dir/prop``
    * ``ns/${NS}``
    * ``ref/${REF}``
  * **POST** Set a property
  * **GET** List properties.

## Meta2 operations

### Container operations
  * URL ``/m2/container``
    * ``ns/${NS}``
  * **HEAD** container existence check
    * ``cid/${CID}`` or ``ref/${REF}``
  * **PUT** container creation. No input expected.
    * ``ref/${REF}``
  * **GET** container listing
    * ``cid/${CID}`` or ``ref/${REF}``
  * **DELETE** container existence check
    * ``cid/${CID}`` or ``ref/${REF}``
  * **POST** additional set of actions
    * ``cid/${CID}`` or ``ref/${REF}``
    * ``action/touch``
    * ``action/purge``
    * ``action/dedup``
    * ``action/stgpol``
	  * ``policy/${STRING}``

### Content operations
  * URL ``/m2/content``
    * ``ns/${NS}``
    * ``ref/${REF}`` or ``cid/${CID}``
    * ``path/${PATH}``
  * **PUT** Store a new set of beans. This set of beans must be a coherent set of aliases.
  * **GET** Fetch the beans belonging to the specified content
  * **HEAD** Check for the content presence
  * **DELETE** 
  * **POST** additional set of actions on contents
    * ``action/beans`` generate some beans
      * ``size/${INT}``
      * ``policy/${POLICY}`` OPTIONAL
    * ``action/copy``
    * ``action/touch``
    * ``action/stgpol``

### Container properties
  * URL ``/m2/container/prop

### Content properties
  * URL ``/m2/content/prop``

## Caches management
  * **GET** only
    * URL ``/cache/status``
  * **POST** only
    * URL ``/cache/flush/low``
    * URL ``/cache/flush/high``
    * URL ``/cache/set/ttl/low/${INT}``
    * URL ``/cache/set/max/low/${INT}``
    * URL ``/cache/set/ttl/high/${INT}``
    * URL ``/cache/set/max/high/${INT}``

## Legacy handlers

### Stateless load-balancing
  * URL ``/lb/sl`` 
    * ``ns/${NS}``
    * ``type/${TYPE}``
  * **GET**
    * Output: A JSON object with a status, and a key ``srv`` pointing to an array of service\_info objects.

### Content location resolution
  * URL ``/m2/get``
    * ``ns/${NS}``
    * ``ref/${REF}``
    * ``path/${PATH}``
  * **GET**
    * Output: A JSON object with a status, and a key ``beans`` pointing to a set of m2v2 beans.

## Payloads
### Array of services
```json
[
  {"addr":"127.0.0.1:22", "score":1, "tags":{ "tag.up":true, }, },
  {"addr":"127.0.0.2:22", "score":1, "tags":{ "tag.up":true, }, },
  {"addr":"127.0.0.3:22", "score":1, "tags":{ "tag.up":true, }, },
]
```
### JSON objet with status and payload
```json
{
  "status":200,
  "message":"",
  $PAYLOAD
}
```
