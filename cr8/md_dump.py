#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argh
import mimetypes
import os
from subprocess import run, PIPE
import logging
import tempfile
import functools
import atexit
import glob
import json
import hashlib

log = logging.getLogger(__file__)
run = functools.partial(run, stdout=PIPE, stderr=PIPE)


def warn_file_not_found(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except FileNotFoundError as e:
            log.warn('File executable not found: %s',
                     getattr(e, 'message', None) or repr(e))
            return None
    return wrapper


def remove(fpath):
    log.info('Removing tmpfile: ' + fpath)
    os.remove(fpath)


def mkstemp(**kwargs):
    fd, tmpfile = tempfile.mkstemp(**kwargs)
    atexit.register(functools.partial(remove, tmpfile))
    return fd, tmpfile


@warn_file_not_found
def tesseract(fpath, lang, **kwargs):
    fd, tmpfile = mkstemp(prefix='tesseract')
    run(['tesseract', fpath, tmpfile, '-l', lang])
    lines = []
    for fpath in glob.glob(tmpfile + '*.txt'):
        with open(fpath, 'r', encoding='utf-8') as f:
            lines += f.readlines()
        os.remove(fpath)
    return '\n'.join(lines)


@warn_file_not_found
def pdf_to_tiff(fpath, **kwargs):
    fd, tmpfile = mkstemp(suffix='.tif')
    run(['gs', '-q', '-dNOPAUSE', '-sDEVICE=tiffg4',
         '-sOutputFile=' + tmpfile, fpath, '-c', 'quit'],
        stdout=PIPE, stderr=PIPE)
    return tmpfile


@warn_file_not_found
def identify(fpath, **kwargs):
    p = run(['identify', '-format', '%wx%h', fpath],
            universal_newlines=True)
    return [int(x) for x in p.stdout.split('x')]


@warn_file_not_found
def pdf_extract(filename, **kwargs):
    p = run(['pdftotext', filename], universal_newlines=True)
    if p.stdout:
        return p.stdout
    return tesseract(pdf_to_tiff(filename, **kwargs), **kwargs)


def no_extract(filename, **kwargs):
    return None


text_extractors = {
    'application/pdf': pdf_extract,
}

identify_types = {'application/pdf'}


def setup_logger():
    ch = logging.StreamHandler()
    log.addHandler(ch)
    log.setLevel(logging.DEBUG)


def compute_digest(fpath):
    with open(fpath, 'rb') as f:
        m = hashlib.sha1()
        for line in f:
            m.update(line)
        return m.hexdigest()


def md_dump(filename, lang='eng', verbose=False):
    """ dumps the content and metadata of a file in JSON format

    This can be used to index the metadata of a file into crate.
    The JSON output can be inserted into crate using the `json2insert` sub-command.

    If the file is a PDF or image, an attempt is made to use pdftotext or
    tesseract (OCR) to extract the text.

    To extract metadata and the content of the file, the following tools need
    to be available:

     - pdftotext
     - ghostscript (gs)
     - tesseract (for OCR)
     - identify (part of imagemagick)
    """
    if verbose:
        setup_logger()
    else:
        log.addHandler(logging.NullHandler())
    mimetype, encoding = mimetypes.guess_type(filename)
    text_extractor = text_extractors.get(mimetype, no_extract)

    if mimetype in identify_types:
        width, height = identify(filename)
    else:
        width, height = None, None
    return json.dumps({
        'filename': os.path.basename(filename),
        'fize': os.path.getsize(filename),
        'mimetype': mimetype,
        'encoding': encoding,
        'content': text_extractor(filename, lang=lang),
        'width': width,
        'height': height,
        'sha1': compute_digest(filename),
        'last_modification': int(os.path.getmtime(filename) * 1000)
    })


def main():
    argh.dispatch_command(md_dump)


if __name__ == "__main__":
    main()
