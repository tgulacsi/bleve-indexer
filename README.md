# Goal
Index my MantisBT attachments, in a searchable format.

# Building blocks
## Bleve
For indexing.

## Apache Tika
For extracting metadata and text from the documents in various formats.

# Structure
A small Go program which 

  * starts Tika in server mode if needed,
  * stops after some time without indexing requests,
  * provides a simple HTTP interface (GET) for searches
  * provides a simple HTTP interface (PUT) indexing documents

A Dockerfile which builds bleve and bleve-indexer, downloads Tika.

# Usage

	go get github.com/tgulacsi/bleve-indexer

It is not that easy - see https://github.com/blevesearch/bleve

## Endpoints

	* PUT/POST to `/add` with `id`: `/add?id=K-MT998:an_important_document.docx`

