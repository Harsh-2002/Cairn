#!/usr/bin/env python3
"""Filename and byte-preservation regressions on disposable API/console nodes."""
import json
import hashlib
from email.message import Message
from email.utils import collapse_rfc2231_value
from urllib.parse import urlsplit

import boto3
from botocore.config import Config

from api_console import expect, node, request


def filename(headers):
    message = Message()
    message["Content-Disposition"] = headers.get("content-disposition", "")
    # RFC 6266 prefers filename* over the ASCII fallback. email exposes RFC 2231 values as tuples.
    for name, value in message.get_params(header="content-disposition", failobj=[]):
        if name == "filename" and isinstance(value, tuple):
            return collapse_rfc2231_value(value)
    return message.get_filename()


def fetch_url(url, method="GET", headers=None):
    parsed = urlsplit(url)
    return request(f"{parsed.scheme}://{parsed.netloc}", method,
                   parsed.path + ("?" + parsed.query if parsed.query else ""), headers=headers)


def client(api, env):
    return boto3.client("s3", endpoint_url=api, region_name="us-east-1",
                        aws_access_key_id=env["CAIRN_ROOT_ACCESS_KEY"],
                        aws_secret_access_key=env["CAIRN_ROOT_SECRET_KEY"],
                        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}))


def check_downloads():
    with node(CAIRN_ENCRYPT_AT_REST="false", CAIRN_FASTIO_MIN_BYTES="0") as (api, console, env):
        auth = {"Authorization": f"Bearer {env['CAIRN_ROOT_ACCESS_KEY']}.{env['CAIRN_ROOT_SECRET_KEY']}"}
        expect(request(api, "POST", "/api/v1/buckets", {"name": "downloads"}, auth), 201)
        payload = b"original file bytes\x00\xff"
        expect(request(api, "PUT", "/downloads/documents/report.pdf", payload, auth), 200)
        share = json.loads(expect(request(api, "POST", "/api/v1/buckets/downloads/objects/shares", {
            "key": "documents/report.pdf", "delivery": "console_download",
        }, auth), 200)[2])
        downloaded = expect(request(console, "GET", urlsplit(share["url"]).path), 200)
        assert filename(downloaded[1]) == "report.pdf", downloaded[1].get("content-disposition")
        assert downloaded[2] == payload
        s3 = client(api, env)
        # All names are raw object keys; percent sequences must never be decoded a second time.
        for key in ["documents/résumé.pdf", "folder/100%2F+done.txt", "folder/no-extension", "empty.bin"]:
            data = b"" if key == "empty.bin" else payload
            s3.put_object(Bucket="downloads", Key=key, Body=data)
            for custom in [None, "", " \t", "..", "custom résumé.txt"]:
                for disposition in ["attachment", "inline"]:
                    share = json.loads(expect(request(api, "POST", "/api/v1/buckets/downloads/objects/shares", {
                        "key": key, "filename": custom, "disposition": disposition,
                    }, auth), 200)[2])
                    expected = custom if custom == "custom résumé.txt" else key.split("/")[-1]
                    path = urlsplit(share["url"]).path
                    for base in [api, console]:
                        for method in ["GET", "HEAD"]:
                            response = expect(request(base, method, path), 200)
                            assert filename(response[1]) == expected, (response[1], expected)
                            assert response[1]["content-disposition"].startswith("attachment" if base == console else disposition)
                            assert response[2] == (data if method == "GET" else b"")
                    cached = expect(request(api, "GET", path, headers={"If-None-Match": response[1]["etag"]}), 304)
                    assert filename(cached[1]) == expected and cached[2] == b""
                    if data:
                        partial = expect(request(console, "GET", path, headers={"Range": "bytes=1-5"}), 206)
                        assert partial[2] == data[1:6] and filename(partial[1]) == expected
                    expect(request(api, "DELETE", f"/api/v1/buckets/downloads/objects/shares/{share['id']}", headers=auth), 204)
                    dead = expect(request(console, "GET", path), 410)
                    assert "content-disposition" not in dead[1]

        # Exceed the blob store's small-read buffer so unencrypted fast-io reads use sendfile.
        read_payload = payload * 32768
        stored = 'attachment; filename="stored.pdf"'
        s3.put_object(Bucket="downloads", Key="metadata.pdf", Body=read_payload, ContentDisposition=stored)
        # SDK-generated overrides exercise the actual percent-encoded wire representation, on both
        # streamed and fast-io binaries. The signature must continue to bind the original query.
        override = "attachment; filename=\"fallback.pdf\"; filename*=UTF-8''r%C3%A9sum%C3%A9%20%25%2B.pdf"
        for method, operation in [("GET", "get_object"), ("HEAD", "head_object")]:
            for supplied in [None, override]:
                params = {"Bucket": "downloads", "Key": "metadata.pdf"}
                if supplied is not None:
                    params["ResponseContentDisposition"] = supplied
                url = s3.generate_presigned_url(operation, Params=params, ExpiresIn=300)
                response = expect(fetch_url(url, method), 200)
                assert response[1]["content-disposition"] == (supplied or stored)
                assert response[2] == (read_payload if method == "GET" else b"")
            minted = json.loads(expect(request(api, "POST", "/api/v1/buckets/downloads/objects/presign", {
                "key": "metadata.pdf", "method": method, "expires_in_secs": 300,
                "response_content_disposition": override, "response_content_type": "application/pdf",
                "origin": console,
            }, auth), 200)[2])
            response = expect(fetch_url(minted["url"], method, {"Origin": console}), 200)
            assert response[1]["content-disposition"] == override
            assert response[1]["content-type"] == "application/pdf"
            assert response[1]["access-control-allow-origin"] == console
            assert "content-disposition" in response[1]["access-control-expose-headers"].lower()
            bad = s3.generate_presigned_url(operation, Params={
                "Bucket": "downloads", "Key": "metadata.pdf",
                "ResponseContentDisposition": "attachment\r\nx-injected: true",
            }, ExpiresIn=300)
            rejected = expect(fetch_url(bad, method), 400)
            assert "x-injected" not in rejected[1]
        url = s3.generate_presigned_url("get_object", Params={
            "Bucket": "downloads", "Key": "metadata.pdf", "ResponseContentDisposition": override,
        }, ExpiresIn=300)
        partial = expect(fetch_url(url, headers={"Range": "bytes=1-300000"}), 206)
        assert partial[1]["content-disposition"] == override and partial[2] == read_payload[1:300001]
        expect(fetch_url(url.replace("fallback.pdf", "changed.pdf")), 403)

        s3.put_bucket_versioning(Bucket="downloads", VersioningConfiguration={"Status": "Enabled"})
        version = s3.put_object(Bucket="downloads", Key="version.pdf", Body=payload)["VersionId"]
        s3.put_object(Bucket="downloads", Key="version.pdf", Body=b"new version")
        pinned = json.loads(expect(request(api, "POST", "/api/v1/buckets/downloads/objects/shares", {
            "key": "version.pdf", "version_id": version, "delivery": "console_download",
        }, auth), 200)[2])
        response = expect(fetch_url(pinned["url"]), 200)
        assert response[2] == payload and filename(response[1]) == "version.pdf"
        missing = json.loads(expect(request(api, "POST", "/api/v1/buckets/downloads/objects/shares", {
            "key": "missing.pdf", "delivery": "console_download",
        }, auth), 200)[2])
        assert "content-disposition" not in expect(fetch_url(missing["url"]), 404)[1]
        metrics = expect(request(api, "GET", "/metrics"), 200)[2].decode()
        if "cairn_sendfile_" in metrics:
            engaged = sum(float(line.rsplit(" ", 1)[1]) for line in metrics.splitlines()
                          if line.startswith("cairn_sendfile_get_total{") and 'result="ok"' in line)
            assert engaged > 0, "fast-io binary did not exercise a download through sendfile"
    print("Download filenames and original bytes passed")


def check_storage_modes():
    for at_rest in [False, True]:
        with node(CAIRN_ENCRYPT_AT_REST=str(at_rest).lower(), CAIRN_KMS_KEY_IDS="downloads") as (api, _, env):
            s3 = client(api, env)
            auth = {"Authorization": f"Bearer {env['CAIRN_ROOT_ACCESS_KEY']}.{env['CAIRN_ROOT_SECRET_KEY']}"}
            s3.create_bucket(Bucket="formats")
            for compression in ["none", "zstd", "lz4"]:
                expect(request(api, "PUT", "/api/v1/buckets/formats/compression", {"algorithm": compression}, auth), 204)
                for encryption in [{}, {"ServerSideEncryption": "AES256"},
                                   {"ServerSideEncryption": "aws:kms", "SSEKMSKeyId": "downloads"}]:
                    data = (b"\x89PNG\r\n\x1a\n\x00\xffcompressed test payload" * 4096)
                    for multipart in [False, True]:
                        key = "folder/original.png"
                        args = {"Bucket": "formats", "Key": key, "ContentType": "image/png",
                                "ContentDisposition": 'attachment; filename="metadata.png"', **encryption}
                        if multipart:
                            upload = s3.create_multipart_upload(**args)["UploadId"]
                            etag = s3.upload_part(Bucket="formats", Key=key, UploadId=upload, PartNumber=1, Body=data)["ETag"]
                            s3.complete_multipart_upload(Bucket="formats", Key=key, UploadId=upload,
                                                         MultipartUpload={"Parts": [{"PartNumber": 1, "ETag": etag}]})
                        else:
                            s3.put_object(**args, Body=data)
                        s3.copy_object(Bucket="formats", Key="copied.png", CopySource={"Bucket": "formats", "Key": key})
                        for object_key in [key, "copied.png"]:
                            direct = s3.get_object(Bucket="formats", Key=object_key)
                            assert direct.get("ContentDisposition") == 'attachment; filename="metadata.png"', (at_rest, compression, encryption, multipart, object_key, direct["ResponseMetadata"]["HTTPHeaders"])
                            assert hashlib.sha256(direct["Body"].read()).digest() == hashlib.sha256(data).digest()
                            share = json.loads(expect(request(api, "POST", "/api/v1/buckets/formats/objects/shares", {
                                "key": object_key, "delivery": "console_download",
                            }, auth), 200)[2])
                            response = expect(fetch_url(share["url"]), 200)
                            assert filename(response[1]) == object_key.split("/")[-1]
                            assert hashlib.sha256(response[2]).digest() == hashlib.sha256(data).digest()
                            partial = expect(fetch_url(share["url"], headers={"Range": "bytes=10-100"}), 206)
                            assert partial[2] == data[10:101]
    print("Plain, encrypted, compressed, multipart and copied download bytes passed")


if __name__ == "__main__":
    check_downloads()
    check_storage_modes()
