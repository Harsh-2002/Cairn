import assert from "node:assert/strict";
import { test } from "node:test";
import { downloadFilename, objectFilename } from "../src/lib/download.ts";

test("object names preserve extensions, Unicode and literal URL characters", () => {
  for (const name of ["report.pdf", "résumé.pdf", "100%2F+done", "no-extension"]) {
    assert.equal(objectFilename(`documents/${name}`), name);
    assert.equal(downloadFilename(null, `documents/${name}`), name);
  }
  for (const key of ["", "folder/", ".", "..", "\r\n"]) {
    assert.equal(objectFilename(key), "download");
  }
});

test("extended UTF-8 filename takes precedence over ASCII fallback", () => {
  assert.equal(downloadFilename('attachment; filename="fallback.pdf"; filename*=UTF-8\'\'r%C3%A9sum%C3%A9%20%22100%25+%22.pdf', "x"), 'résumé "100%+".pdf');
  assert.equal(downloadFilename('inline; FILENAME*=utf-8\'en\'actual.txt', "x"), "actual.txt");
  assert.equal(downloadFilename('attachment; filename="literal%2F.txt"', "x"), "literal%2F.txt");
});

test("quoted parameters, invalid encodings and unsafe names", () => {
  assert.equal(downloadFilename('attachment; filename="a; \\"quoted\\".txt"', "x"), 'a; "quoted".txt');
  for (const extended of ["UTF-8''%ZZ", "UTF-8''%FF", "unsupported''abc", "UTF-8''.."]) {
    assert.equal(downloadFilename(`attachment; filename*= ${extended}; filename="fallback.txt"`, "x"), "fallback.txt");
  }
  assert.equal(downloadFilename('attachment; filename="../../safe.txt"', "x"), "safe.txt");
  for (const header of ['attachment', 'attachment; filename=""', 'attachment; filename=".."', 'attachment; filename="unterminated', 'attachment; filename=a; filename=b']) {
    assert.equal(downloadFilename(header, "folder/default.txt"), "default.txt");
  }
});
