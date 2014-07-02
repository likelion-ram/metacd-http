# metacd-http

RedCurrant HTTP proxy for META{0,1,2} services

## Conventions

We talk about *URL prefix* and *URL options*. This concern the path of the URL. The options are concatened using '/'.

In the following description, a few ``${...}`` elements are presented. For each element, here are the constraints on the element's values:
  * ``${NS}`` : a known namespace, typically a string with the form of a domain name. The physical namespace is the main token when read from left to right. According to Redcurrant's standards, a namespace token should not contain any character that is not alphanumerical.
  * ``${TYPE}`` :  a service type, single (e.g. ``meta2``) or compound (e.g. ``sqlx.plop``). the physical service type is the forst token when read from left to right. According to Redcurrant's standards, a service type's  token should not contain any character that is not alphanumerical.
  * ``${REF}`` : 
  * ``${PATH}`` : 
  * ``${POLICY}`` : a storage policy
  * ``${INT}`` : an integer in decimal form.

## Conscience operations

### Stateless load-balancing
  * Method: ``GET``
  * URL Prefix: ``/lb/sl`` 
  * URL Options:
    * ``ns/${NS}``
    * ``type/${TYPE}``

## Meta2 operations

### Content location resolution
  * Method: ``GET``
  * URL Prefix: ``/m2/get``
  * URL Options:
    * ``ns/${NS}``
    * ``ref/${REF}``
    * ``path/${PATH}``

### Content location provision
  * Method: ``GET``
  * URL Prefix: ``/m2/beans``
  * URL Options:
    * ``ns/${NS}``
    * ``ref/${REF}``
    * ``path/${PATH}``
    * ``size/${INT}``
    * ``policy/${POLICY}`` OPTIONAL

### Whole container listing
  * Method: ``GET``
  * URL Prefix: ``/m2/list``
  * URL Options:
    * ``ns/${NS}``
    * ``ref/${REF}``

### Container's presence check
  * Method: ``GET``
  * URL Prefix: ``/m2/has``
  * URL Options:
    * ``ns/${NS}``
    * ``ref/${REF}``

### Create a container
  * Method: ``POST``
  * URL Prefix: ``/m2/create``
  * URL Options:
    * ``ns/${NS}``
    * ``ref/${REF}``

### Destroy a container
  * Method: ``POST``
  * URL Prefix: ``/m2/destroy``
  * URL Options:
    * ``ns/${NS}``
    * ``ref/${REF}``

### Open a container
  * Method: ``POST``
  * URL Prefix: ``/m2/open``
  * URL Options:
    * ``ns/${NS}``
    * ``ref/${REF}``

### Close a container
  * Method: ``POST``
  * URL Prefix: ``/m2/close``
  * URL Options:
    * ``ns/${NS}``
    * ``ref/${REF}``

### Purge a container
  * Method: ``POST``
  * URL Prefix: ``/m2/purge``
  * URL Options:
    * ``ns/${NS}``
    * ``ref/${REF}``

### Deduplicate a container
  * Method: ``POST``
  * URL Prefix: ``/m2/dedup``
  * URL Options:
    * ``ns/${NS}``
    * ``ref/${REF}``

### Copy a content
  * Method: ``POST``
  * URL Prefix: ``/m2/copy``
  * URL Options:
    * ``ns/${NS}``
    * ``ref/${REF}``
    * ``path/${PATH}`` OPTIONAL, if present it concerns the content in the container.

### Touch a container of a content
  * Method: ``POST``
  * URL Prefix: ``/m2/touch``
  * URL Options:
    * ``ns/${NS}``
    * ``ref/${REF}``
    * ``path/${PATH}`` OPTIONAL, if present it concerns the content in the container.

### Storage policy change for a container or a content
  * Method: ``POST``
  * URL Prefix: ``/m2/stgpol``
  * URL Options:
    * ``ns/${NS}``
    * ``ref/${REF}``
    * ``path/${PATH}`` OPTIONAL, if present it concerns the content in the container.

### Content PUT
  * Method: ``POST``
  * URL Prefix: ``/m2/put``
  * URL Options:
    * ``ns/${NS}``
    * ``ref/${REF}``
    * ``path/${PATH}``

### Content APPEND
  * Method: ``POST``
  * URL Prefix: ``/m2/append``
  * URL Options:
    * ``ns/${NS}``
    * ``ref/${REF}``
    * ``path/${PATH}``

## High-level Directory operations

### Container service listing
  * Method: ``GET``
  * URL Prefix: ``/dir/list``
  * URL Options:
    * ``ns/${NS}``
    * ``ref/${REF}``
    * ``type/${TYPE}``

### Simple stats about the directory cache usage
  * Method: ``GET``
  * URL Prefix: ``/dir/status``

### Flush the meta1's cache.
  * Method: ``POST``
  * URL Prefix: ``/dir/flush/low``

### Flush the conscience + meta0's cache.
  * Method: ``POST``
  * URL Prefix: ``/dir/flush/high``

### Set various limits on internal caches
  * Method: ``POST``
  * URL Prefix: ``/dir/set/ttl/low``
  * URL Prefix: ``/dir/set/max/low``
  * URL Prefix: ``/dir/set/ttl/high``
  * URL Prefix: ``/dir/set/max/high``
  * URL Options:
    * ${INT}
