create table docs (
    sha1 string primary key,
    content string index using fulltext with (analyzer = 'german'),
    mimetype string,
    size long,
    width int,
    height int,
    tags array(string),
    filename string,
    last_modification timestamp
) with (number_of_replicas = '0-1');
