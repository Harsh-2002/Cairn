# ZAP Scanning Report

ZAP by [Checkmarx](https://checkmarx.com/).


## Summary of Alerts

| Risk Level | Number of Alerts |
| --- | --- |
| High | 1 |
| Medium | 2 |
| Low | 1 |
| Informational | 5 |




## Insights

| Level | Reason | Site | Description | Statistic |
| --- | --- | --- | --- | --- |
| Low | Warning |  | ZAP warnings logged - see the zap.log file for details | 23    |
| Info | Informational | http://127.0.0.1:9444 | Percentage of responses with status code 3xx | 100 % |
| Info | Informational | https://127.0.0.1:9444 | Percentage of responses with status code 2xx | 58 % |
| Info | Exceeded Low | https://127.0.0.1:9444 | Percentage of responses with status code 4xx | 41 % |
| Info | Informational | https://127.0.0.1:9444 | Percentage of endpoints with content type application/json | 38 % |
| Info | Informational | https://127.0.0.1:9444 | Percentage of endpoints with content type font/woff2 | 3 % |
| Info | Informational | https://127.0.0.1:9444 | Percentage of endpoints with content type image/svg+xml | 1 % |
| Info | Informational | https://127.0.0.1:9444 | Percentage of endpoints with content type text/css | 1 % |
| Info | Informational | https://127.0.0.1:9444 | Percentage of endpoints with content type text/event-stream | 1 % |
| Info | Informational | https://127.0.0.1:9444 | Percentage of endpoints with content type text/html | 1 % |
| Info | Informational | https://127.0.0.1:9444 | Percentage of endpoints with content type text/javascript | 54 % |
| Info | Informational | https://127.0.0.1:9444 | Percentage of endpoints with method GET | 91 % |
| Info | Informational | https://127.0.0.1:9444 | Percentage of endpoints with method POST | 8 % |
| Info | Informational | https://127.0.0.1:9444 | Count of total endpoints | 109    |







## Alerts

| Name | Risk Level | Number of Instances |
| --- | --- | --- |
| Source Code Disclosure - File Inclusion | High | 21 |
| CSP: style-src unsafe-inline | Medium | 2 |
| Proxy Disclosure | Medium | Systemic |
| Cookie Slack Detector | Low | Systemic |
| Information Disclosure - Sensitive Information in URL | Informational | 1 |
| Modern Web Application | Informational | 2 |
| Re-examine Cache-control Directives | Informational | Systemic |
| Storable and Cacheable Content | Informational | Systemic |
| User Agent Fuzzer | Informational | Systemic |




## Alert Detail



### [ Source Code Disclosure - File Inclusion ](https://www.zaproxy.org/docs/alerts/43/)



##### High (Medium)

### Description

The Path Traversal attack technique allows an attacker access to files, directories, and commands that potentially reside outside the web document root directory. An attacker may manipulate a URL in such a way that the web site will execute or reveal the contents of arbitrary files anywhere on the web server. Any device that exposes an HTTP-based interface is potentially vulnerable to Path Traversal.

Most web sites restrict user access to a specific portion of the file-system, typically called the "web document root" or "CGI root" directory. These directories contain the files intended for user access and the executable necessary to drive web application functionality. To access files or execute commands anywhere on the file-system, Path Traversal attacks will utilize the ability of special-characters sequences.

The most basic Path Traversal attack uses the "../" special-character sequence to alter the resource location requested in the URL. Although most popular web servers will prevent this technique from escaping the web document root, alternate encodings of the "../" sequence may help bypass the security filters. These method variations include valid and invalid Unicode-encoding ("..%u2216" or "..%c0%af") of the forward slash character, backslash characters ("..\") on Windows-based servers, URL encoded characters "%2e%2e%2f"), and double URL encoding ("..%255c") of the backslash character.

Even if the web server properly restricts Path Traversal attempts in the URL path, a web application itself may still be vulnerable due to improper handling of user-supplied input. This is a common problem of web applications that use template mechanisms or load static text from files. In variations of the attack, the original URL parameter value is substituted with the file name of one of the web application's dynamic scripts. Consequently, the results can reveal source code because the file is interpreted as text instead of an executable script. These techniques often employ additional special characters such as the dot (".") to reveal the listing of the current working directory, or "%00" NULL characters in order to bypass rudimentary file extension checks.

* URL: https://127.0.0.1:9444/api/v1/imports%3Flimit=1000
  * Node Name: `https://127.0.0.1:9444/api/v1/imports (limit)`
  * Method: `GET`
  * Parameter: `limit`
  * Attack: `imports`
  * Evidence: ``
  * Other Info: `The output for the source code filename [imports] differs sufficiently from that of the random parameter [tpckrdnrvjceqyazpeagotwpbcfrqrqlseszil], at [70%], compared to a threshold of [75%]`
* URL: https://127.0.0.1:9444/api/v1/buckets/pentest-lock/objects/presign
  * Node Name: `https://127.0.0.1:9444/api/v1/buckets/pentest-lock/objects/presign ()({key,method,expires_in_secs,query:[[]],headers:[],origin,session:{access_key_id,session_token}})`
  * Method: `POST`
  * Parameter: `access_key_id`
  * Attack: `presign`
  * Evidence: ``
  * Other Info: `The output for the source code filename [presign] differs sufficiently from that of the random parameter [tpckrdnrvjceqyazpeagotwpbcfrqrqlseszil], at [71%], compared to a threshold of [75%]`
* URL: https://127.0.0.1:9444/api/v1/buckets/pentest-lock/objects/presign
  * Node Name: `https://127.0.0.1:9444/api/v1/buckets/pentest-lock/objects/presign ()({key,method,expires_in_secs,query:[[]],headers:[],origin,session:{access_key_id,session_token}})`
  * Method: `POST`
  * Parameter: `session_token`
  * Attack: `presign`
  * Evidence: ``
  * Other Info: `The output for the source code filename [presign] differs sufficiently from that of the random parameter [tpckrdnrvjceqyazpeagotwpbcfrqrqlseszil], at [73%], compared to a threshold of [75%]`
* URL: https://127.0.0.1:9444/api/v1/buckets/pentest-lock/objects/presign
  * Node Name: `https://127.0.0.1:9444/api/v1/buckets/pentest-lock/objects/presign ()({key,method,expires_in_secs,query:[[]],headers:[],origin})`
  * Method: `POST`
  * Parameter: `key`
  * Attack: `presign`
  * Evidence: ``
  * Other Info: `The output for the source code filename [presign] differs sufficiently from that of the random parameter [tpckrdnrvjceqyazpeagotwpbcfrqrqlseszil], at [70%], compared to a threshold of [75%]`
* URL: https://127.0.0.1:9444/api/v1/buckets/pentest-lock/objects/presign
  * Node Name: `https://127.0.0.1:9444/api/v1/buckets/pentest-lock/objects/presign ()({key,method,expires_in_secs,query:[[]],headers:[],origin})`
  * Method: `POST`
  * Parameter: `query[0][0]`
  * Attack: `presign`
  * Evidence: ``
  * Other Info: `The output for the source code filename [presign] differs sufficiently from that of the random parameter [tpckrdnrvjceqyazpeagotwpbcfrqrqlseszil], at [72%], compared to a threshold of [75%]`
* URL: https://127.0.0.1:9444/api/v1/buckets/pentest-lock/objects/presign
  * Node Name: `https://127.0.0.1:9444/api/v1/buckets/pentest-lock/objects/presign ()({key,method,expires_in_secs,query:[[]],headers:[],origin})`
  * Method: `POST`
  * Parameter: `query[0][1]`
  * Attack: `presign`
  * Evidence: ``
  * Other Info: `The output for the source code filename [presign] differs sufficiently from that of the random parameter [tpckrdnrvjceqyazpeagotwpbcfrqrqlseszil], at [71%], compared to a threshold of [75%]`
* URL: https://127.0.0.1:9444/api/v1/buckets/pentest-private/objects/presign
  * Node Name: `https://127.0.0.1:9444/api/v1/buckets/pentest-private/objects/presign ()({key,method,expires_in_secs,query:[[]],headers:[],origin,session:{access_key_id,session_token}})`
  * Method: `POST`
  * Parameter: `access_key_id`
  * Attack: `presign`
  * Evidence: ``
  * Other Info: `The output for the source code filename [presign] differs sufficiently from that of the random parameter [tpckrdnrvjceqyazpeagotwpbcfrqrqlseszil], at [73%], compared to a threshold of [75%]`
* URL: https://127.0.0.1:9444/api/v1/buckets/pentest-private/objects/presign
  * Node Name: `https://127.0.0.1:9444/api/v1/buckets/pentest-private/objects/presign ()({key,method,expires_in_secs,query:[[]],headers:[],origin,session:{access_key_id,session_token}})`
  * Method: `POST`
  * Parameter: `session_token`
  * Attack: `presign`
  * Evidence: ``
  * Other Info: `The output for the source code filename [presign] differs sufficiently from that of the random parameter [tpckrdnrvjceqyazpeagotwpbcfrqrqlseszil], at [74%], compared to a threshold of [75%]`
* URL: https://127.0.0.1:9444/api/v1/buckets/pentest-private/objects/presign
  * Node Name: `https://127.0.0.1:9444/api/v1/buckets/pentest-private/objects/presign ()({key,method,expires_in_secs,query:[[]],headers:[],origin})`
  * Method: `POST`
  * Parameter: `key`
  * Attack: `presign`
  * Evidence: ``
  * Other Info: `The output for the source code filename [presign] differs sufficiently from that of the random parameter [tpckrdnrvjceqyazpeagotwpbcfrqrqlseszil], at [69%], compared to a threshold of [75%]`
* URL: https://127.0.0.1:9444/api/v1/buckets/pentest-private/objects/presign
  * Node Name: `https://127.0.0.1:9444/api/v1/buckets/pentest-private/objects/presign ()({key,method,expires_in_secs,query:[[]],headers:[],origin})`
  * Method: `POST`
  * Parameter: `query[0][0]`
  * Attack: `presign`
  * Evidence: ``
  * Other Info: `The output for the source code filename [presign] differs sufficiently from that of the random parameter [tpckrdnrvjceqyazpeagotwpbcfrqrqlseszil], at [70%], compared to a threshold of [75%]`
* URL: https://127.0.0.1:9444/api/v1/buckets/pentest-private/objects/presign
  * Node Name: `https://127.0.0.1:9444/api/v1/buckets/pentest-private/objects/presign ()({key,method,expires_in_secs,query:[[]],headers:[],origin})`
  * Method: `POST`
  * Parameter: `query[0][1]`
  * Attack: `presign`
  * Evidence: ``
  * Other Info: `The output for the source code filename [presign] differs sufficiently from that of the random parameter [tpckrdnrvjceqyazpeagotwpbcfrqrqlseszil], at [71%], compared to a threshold of [75%]`
* URL: https://127.0.0.1:9444/api/v1/buckets/pentest-public/objects/presign
  * Node Name: `https://127.0.0.1:9444/api/v1/buckets/pentest-public/objects/presign ()({key,method,expires_in_secs,query:[[]],headers:[],origin,session:{access_key_id,session_token}})`
  * Method: `POST`
  * Parameter: `access_key_id`
  * Attack: `presign`
  * Evidence: ``
  * Other Info: `The output for the source code filename [presign] differs sufficiently from that of the random parameter [tpckrdnrvjceqyazpeagotwpbcfrqrqlseszil], at [73%], compared to a threshold of [75%]`
* URL: https://127.0.0.1:9444/api/v1/buckets/pentest-public/objects/presign
  * Node Name: `https://127.0.0.1:9444/api/v1/buckets/pentest-public/objects/presign ()({key,method,expires_in_secs,query:[[]],headers:[],origin,session:{access_key_id,session_token}})`
  * Method: `POST`
  * Parameter: `session_token`
  * Attack: `presign`
  * Evidence: ``
  * Other Info: `The output for the source code filename [presign] differs sufficiently from that of the random parameter [tpckrdnrvjceqyazpeagotwpbcfrqrqlseszil], at [74%], compared to a threshold of [75%]`
* URL: https://127.0.0.1:9444/api/v1/buckets/pentest-public/objects/presign
  * Node Name: `https://127.0.0.1:9444/api/v1/buckets/pentest-public/objects/presign ()({key,method,expires_in_secs,query:[[]],headers:[],origin})`
  * Method: `POST`
  * Parameter: `key`
  * Attack: `presign`
  * Evidence: ``
  * Other Info: `The output for the source code filename [presign] differs sufficiently from that of the random parameter [tpckrdnrvjceqyazpeagotwpbcfrqrqlseszil], at [72%], compared to a threshold of [75%]`
* URL: https://127.0.0.1:9444/api/v1/buckets/pentest-public/objects/presign
  * Node Name: `https://127.0.0.1:9444/api/v1/buckets/pentest-public/objects/presign ()({key,method,expires_in_secs,query:[[]],headers:[],origin})`
  * Method: `POST`
  * Parameter: `query[0][0]`
  * Attack: `presign`
  * Evidence: ``
  * Other Info: `The output for the source code filename [presign] differs sufficiently from that of the random parameter [tpckrdnrvjceqyazpeagotwpbcfrqrqlseszil], at [70%], compared to a threshold of [75%]`
* URL: https://127.0.0.1:9444/api/v1/buckets/pentest-public/objects/presign
  * Node Name: `https://127.0.0.1:9444/api/v1/buckets/pentest-public/objects/presign ()({key,method,expires_in_secs,query:[[]],headers:[],origin})`
  * Method: `POST`
  * Parameter: `query[0][1]`
  * Attack: `presign`
  * Evidence: ``
  * Other Info: `The output for the source code filename [presign] differs sufficiently from that of the random parameter [tpckrdnrvjceqyazpeagotwpbcfrqrqlseszil], at [70%], compared to a threshold of [75%]`
* URL: https://127.0.0.1:9444/api/v1/buckets/pentest-versioned/objects/presign
  * Node Name: `https://127.0.0.1:9444/api/v1/buckets/pentest-versioned/objects/presign ()({key,method,expires_in_secs,query:[[]],headers:[],origin,session:{access_key_id,session_token}})`
  * Method: `POST`
  * Parameter: `access_key_id`
  * Attack: `presign`
  * Evidence: ``
  * Other Info: `The output for the source code filename [presign] differs sufficiently from that of the random parameter [tpckrdnrvjceqyazpeagotwpbcfrqrqlseszil], at [73%], compared to a threshold of [75%]`
* URL: https://127.0.0.1:9444/api/v1/buckets/pentest-versioned/objects/presign
  * Node Name: `https://127.0.0.1:9444/api/v1/buckets/pentest-versioned/objects/presign ()({key,method,expires_in_secs,query:[[]],headers:[],origin,session:{access_key_id,session_token}})`
  * Method: `POST`
  * Parameter: `session_token`
  * Attack: `presign`
  * Evidence: ``
  * Other Info: `The output for the source code filename [presign] differs sufficiently from that of the random parameter [tpckrdnrvjceqyazpeagotwpbcfrqrqlseszil], at [73%], compared to a threshold of [75%]`
* URL: https://127.0.0.1:9444/api/v1/buckets/pentest-versioned/objects/presign
  * Node Name: `https://127.0.0.1:9444/api/v1/buckets/pentest-versioned/objects/presign ()({key,method,expires_in_secs,query:[[]],headers:[],origin})`
  * Method: `POST`
  * Parameter: `key`
  * Attack: `presign`
  * Evidence: ``
  * Other Info: `The output for the source code filename [presign] differs sufficiently from that of the random parameter [tpckrdnrvjceqyazpeagotwpbcfrqrqlseszil], at [69%], compared to a threshold of [75%]`
* URL: https://127.0.0.1:9444/api/v1/buckets/pentest-versioned/objects/presign
  * Node Name: `https://127.0.0.1:9444/api/v1/buckets/pentest-versioned/objects/presign ()({key,method,expires_in_secs,query:[[]],headers:[],origin})`
  * Method: `POST`
  * Parameter: `query[0][0]`
  * Attack: `presign`
  * Evidence: ``
  * Other Info: `The output for the source code filename [presign] differs sufficiently from that of the random parameter [tpckrdnrvjceqyazpeagotwpbcfrqrqlseszil], at [71%], compared to a threshold of [75%]`
* URL: https://127.0.0.1:9444/api/v1/buckets/pentest-versioned/objects/presign
  * Node Name: `https://127.0.0.1:9444/api/v1/buckets/pentest-versioned/objects/presign ()({key,method,expires_in_secs,query:[[]],headers:[],origin})`
  * Method: `POST`
  * Parameter: `query[0][1]`
  * Attack: `presign`
  * Evidence: ``
  * Other Info: `The output for the source code filename [presign] differs sufficiently from that of the random parameter [tpckrdnrvjceqyazpeagotwpbcfrqrqlseszil], at [69%], compared to a threshold of [75%]`


Instances: 21

### Solution

Assume all input is malicious. Use an "accept known good" input validation strategy, i.e., use an allow list of acceptable inputs that strictly conform to specifications. Reject any input that does not strictly conform to specifications, or transform it into something that does. Do not rely exclusively on looking for malicious or malformed inputs (i.e., do not rely on a deny list). However, deny lists can be useful for detecting potential attacks or determining which inputs are so malformed that they should be rejected outright.

When performing input validation, consider all potentially relevant properties, including length, type of input, the full range of acceptable values, missing or extra inputs, syntax, consistency across related fields, and conformance to business rules. As an example of business rule logic, "boat" may be syntactically valid because it only contains alphanumeric characters, but it is not valid if you are expecting colors such as "red" or "blue."

For filenames, use stringent allow lists that limit the character set to be used. If feasible, only allow a single "." character in the filename to avoid weaknesses, and exclude directory separators such as "/". Use an allow list of allowable file extensions.

Warning: if you attempt to cleanse your data, then do so that the end result is not in the form that can be dangerous. A sanitizing mechanism can remove characters such as '.' and ';' which may be required for some exploits. An attacker can try to fool the sanitizing mechanism into "cleaning" data into a dangerous form. Suppose the attacker injects a '.' inside a filename (e.g. "sensi.tiveFile") and the sanitizing mechanism removes the character resulting in the valid filename, "sensitiveFile". If the input data are now assumed to be safe, then the file may be compromised. 

Inputs should be decoded and canonicalized to the application's current internal representation before being validated. Make sure that your application does not decode the same input twice. Such errors could be used to bypass allow list schemes by introducing dangerous inputs after they have been checked.

Use a built-in path canonicalization function (such as realpath() in C) that produces the canonical version of the pathname, which effectively removes ".." sequences and symbolic links.

Run your code using the lowest privileges that are required to accomplish the necessary tasks. If possible, create isolated accounts with limited privileges that are only used for a single task. That way, a successful attack will not immediately give the attacker access to the rest of the software or its environment. For example, database applications rarely need to run as the database administrator, especially in day-to-day operations.

When the set of acceptable objects, such as filenames or URLs, is limited or known, create a mapping from a set of fixed input values (such as numeric IDs) to the actual filenames or URLs, and reject all other inputs.

Run your code in a "jail" or similar sandbox environment that enforces strict boundaries between the process and the operating system. This may effectively restrict which files can be accessed in a particular directory or which commands can be executed by your software.

OS-level examples include the Unix chroot jail, AppArmor, and SELinux. In general, managed code may provide some protection. For example, java.io.FilePermission in the Java SecurityManager allows you to specify restrictions on file operations.

This may not be a feasible solution, and it only limits the impact to the operating system; the rest of your application may still be subject to compromise.


### Reference


* [ https://owasp.org/www-community/attacks/Path_Traversal ](https://owasp.org/www-community/attacks/Path_Traversal)
* [ https://cwe.mitre.org/data/definitions/22.html ](https://cwe.mitre.org/data/definitions/22.html)


#### CWE Id: [ 541 ](https://cwe.mitre.org/data/definitions/541.html)


#### WASC Id: 33

#### Source ID: 1

### [ CSP: style-src unsafe-inline ](https://www.zaproxy.org/docs/alerts/10055/)



##### Medium (High)

### Description

Content Security Policy (CSP) is an added layer of security that helps to detect and mitigate certain types of attacks. Including (but not limited to) Cross Site Scripting (XSS), and data injection attacks. These attacks are used for everything from data theft to site defacement or distribution of malware. CSP provides a set of standard HTTP headers that allow website owners to declare approved sources of content that browsers should be allowed to load on that page — covered types are JavaScript, CSS, HTML frames, fonts, images and embeddable objects such as Java applets, ActiveX, audio and video files.

* URL: https://127.0.0.1:9444
  * Node Name: `https://127.0.0.1:9444`
  * Method: `GET`
  * Parameter: `content-security-policy`
  * Attack: ``
  * Evidence: `default-src 'self'; base-uri 'none'; object-src 'none'; frame-ancestors 'none'; form-action 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data: blob:; font-src 'self' data:; connect-src 'self'`
  * Other Info: `style-src includes unsafe-inline.`
* URL: https://127.0.0.1:9444/
  * Node Name: `https://127.0.0.1:9444/`
  * Method: `GET`
  * Parameter: `content-security-policy`
  * Attack: ``
  * Evidence: `default-src 'self'; base-uri 'none'; object-src 'none'; frame-ancestors 'none'; form-action 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data: blob:; font-src 'self' data:; connect-src 'self'`
  * Other Info: `style-src includes unsafe-inline.`


Instances: 2

### Solution

Ensure that your web server, application server, load balancer, etc. is properly configured to set the Content-Security-Policy header.

### Reference


* [ https://www.w3.org/TR/CSP/ ](https://www.w3.org/TR/CSP/)
* [ https://caniuse.com/#search=content+security+policy ](https://caniuse.com/#search=content+security+policy)
* [ https://content-security-policy.com/ ](https://content-security-policy.com/)
* [ https://github.com/HtmlUnit/htmlunit-csp ](https://github.com/HtmlUnit/htmlunit-csp)
* [ https://web.dev/articles/csp#resource-options ](https://web.dev/articles/csp#resource-options)


#### CWE Id: [ 693 ](https://cwe.mitre.org/data/definitions/693.html)


#### WASC Id: 15

#### Source ID: 3

### [ Proxy Disclosure ](https://www.zaproxy.org/docs/alerts/40025/)



##### Medium (Medium)

### Description

1 proxy server(s) were detected or fingerprinted. This information helps a potential attacker to determine
- A list of targets for an attack against the application.
 - Potential vulnerabilities on the proxy servers that service the application.
 - The presence or absence of any proxy-based components that might cause attacks against the application to be detected, prevented, or mitigated.

* URL: https://127.0.0.1:9444/
  * Node Name: `https://127.0.0.1:9444/`
  * Method: `GET`
  * Parameter: ``
  * Attack: `TRACE, OPTIONS methods with 'Max-Forwards' header. TRACK method.`
  * Evidence: ``
  * Other Info: `Using the TRACE, OPTIONS, and TRACK methods, the following proxy servers have been identified between ZAP and the application/web server:
- Unknown
The following web/application server has been identified:
- Unknown
`
* URL: https://127.0.0.1:9444/api/v1/buckets/pentest-lock/objects/presign
  * Node Name: `https://127.0.0.1:9444/api/v1/buckets/pentest-lock/objects/presign ()({key,method,expires_in_secs,query:[[]],headers:[],origin,session:{access_key_id,session_token}})`
  * Method: `POST`
  * Parameter: ``
  * Attack: `TRACE, OPTIONS methods with 'Max-Forwards' header. TRACK method.`
  * Evidence: ``
  * Other Info: `Using the TRACE, OPTIONS, and TRACK methods, the following proxy servers have been identified between ZAP and the application/web server:
- Unknown
The following web/application server has been identified:
- Unknown
`
* URL: https://127.0.0.1:9444/api/v1/buckets/pentest-lock/objects/presign
  * Node Name: `https://127.0.0.1:9444/api/v1/buckets/pentest-lock/objects/presign ()({key,method,expires_in_secs,query:[[]],headers:[],origin})`
  * Method: `POST`
  * Parameter: ``
  * Attack: `TRACE, OPTIONS methods with 'Max-Forwards' header. TRACK method.`
  * Evidence: ``
  * Other Info: `Using the TRACE, OPTIONS, and TRACK methods, the following proxy servers have been identified between ZAP and the application/web server:
- Unknown
The following web/application server has been identified:
- Unknown
`
* URL: https://127.0.0.1:9444/api/v1/buckets/pentest-private/objects/presign
  * Node Name: `https://127.0.0.1:9444/api/v1/buckets/pentest-private/objects/presign ()({key,method,expires_in_secs,query:[[]],headers:[],origin,session:{access_key_id,session_token}})`
  * Method: `POST`
  * Parameter: ``
  * Attack: `TRACE, OPTIONS methods with 'Max-Forwards' header. TRACK method.`
  * Evidence: ``
  * Other Info: `Using the TRACE, OPTIONS, and TRACK methods, the following proxy servers have been identified between ZAP and the application/web server:
- Unknown
The following web/application server has been identified:
- Unknown
`
* URL: https://127.0.0.1:9444/api/v1/events/ticket
  * Node Name: `https://127.0.0.1:9444/api/v1/events/ticket`
  * Method: `POST`
  * Parameter: ``
  * Attack: `TRACE, OPTIONS methods with 'Max-Forwards' header. TRACK method.`
  * Evidence: ``
  * Other Info: `Using the TRACE, OPTIONS, and TRACK methods, the following proxy servers have been identified between ZAP and the application/web server:
- Unknown
The following web/application server has been identified:
- Unknown
`

Instances: Systemic


### Solution

Disable the 'TRACE' method on the proxy servers, as well as the origin web/application server.
Disable the 'OPTIONS' method on the proxy servers, as well as the origin web/application server, if it is not required for other purposes, such as 'CORS' (Cross Origin Resource Sharing).
Configure the web and application servers with custom error pages, to prevent 'fingerprintable' product-specific error pages being leaked to the user in the event of HTTP errors, such as 'TRACK' requests for non-existent pages.
Configure all proxies, application servers, and web servers to prevent disclosure of the technology and version information in the 'Server' and 'X-Powered-By' HTTP response headers.


### Reference


* [ https://datatracker.ietf.org/doc/html/rfc7231#section-5.1.2 ](https://datatracker.ietf.org/doc/html/rfc7231#section-5.1.2)


#### CWE Id: [ 204 ](https://cwe.mitre.org/data/definitions/204.html)


#### WASC Id: 45

#### Source ID: 1

### [ Cookie Slack Detector ](https://www.zaproxy.org/docs/alerts/90027/)



##### Low (Low)

### Description

Repeated GET requests: drop a different cookie each time, followed by normal request with all cookies to stabilize session, compare responses against original baseline GET. This can reveal areas where cookie based authentication/attributes are not actually enforced.

* URL: https://127.0.0.1:9444
  * Node Name: `https://127.0.0.1:9444`
  * Method: `GET`
  * Parameter: ``
  * Attack: ``
  * Evidence: ``
  * Other Info: `NOTE: Because of its name this cookie may be important, but dropping it appears to have no effect: [cairn_session]
Cookies that don't have expected effects can reveal flaws in application logic. In the worst case, this can reveal where authentication via cookie token(s) is not actually enforced.
These cookies affected the response: 
These cookies did NOT affect the response: cairn_session
`
* URL: https://127.0.0.1:9444/
  * Node Name: `https://127.0.0.1:9444/`
  * Method: `GET`
  * Parameter: ``
  * Attack: ``
  * Evidence: ``
  * Other Info: `NOTE: Because of its name this cookie may be important, but dropping it appears to have no effect: [cairn_session]
Cookies that don't have expected effects can reveal flaws in application logic. In the worst case, this can reveal where authentication via cookie token(s) is not actually enforced.
These cookies affected the response: 
These cookies did NOT affect the response: cairn_session
`
* URL: https://127.0.0.1:9444/api/v1/buckets/pentest-lock/objects/presign
  * Node Name: `https://127.0.0.1:9444/api/v1/buckets/pentest-lock/objects/presign ()({key,method,expires_in_secs,query:[[]],headers:[],origin,session:{access_key_id,session_token}})`
  * Method: `POST`
  * Parameter: ``
  * Attack: ``
  * Evidence: ``
  * Other Info: `NOTE: Because of its name this cookie may be important, but dropping it appears to have no effect: [cairn_session]
Cookies that don't have expected effects can reveal flaws in application logic. In the worst case, this can reveal where authentication via cookie token(s) is not actually enforced.
These cookies affected the response: 
These cookies did NOT affect the response: cairn_session
`
* URL: https://127.0.0.1:9444/api/v1/buckets/pentest-private/objects/presign
  * Node Name: `https://127.0.0.1:9444/api/v1/buckets/pentest-private/objects/presign ()({key,method,expires_in_secs,query:[[]],headers:[],origin,session:{access_key_id,session_token}})`
  * Method: `POST`
  * Parameter: ``
  * Attack: ``
  * Evidence: ``
  * Other Info: `NOTE: Because of its name this cookie may be important, but dropping it appears to have no effect: [cairn_session]
Cookies that don't have expected effects can reveal flaws in application logic. In the worst case, this can reveal where authentication via cookie token(s) is not actually enforced.
These cookies affected the response: 
These cookies did NOT affect the response: cairn_session
`
* URL: https://127.0.0.1:9444/api/v1/buckets/pentest-public/objects/presign
  * Node Name: `https://127.0.0.1:9444/api/v1/buckets/pentest-public/objects/presign ()({key,method,expires_in_secs,query:[[]],headers:[],origin,session:{access_key_id,session_token}})`
  * Method: `POST`
  * Parameter: ``
  * Attack: ``
  * Evidence: ``
  * Other Info: `NOTE: Because of its name this cookie may be important, but dropping it appears to have no effect: [cairn_session]
Cookies that don't have expected effects can reveal flaws in application logic. In the worst case, this can reveal where authentication via cookie token(s) is not actually enforced.
These cookies affected the response: 
These cookies did NOT affect the response: cairn_session
`

Instances: Systemic


### Solution



### Reference


* [ https://cwe.mitre.org/data/definitions/205.html ](https://cwe.mitre.org/data/definitions/205.html)


#### CWE Id: [ 205 ](https://cwe.mitre.org/data/definitions/205.html)


#### WASC Id: 45

#### Source ID: 1

### [ Information Disclosure - Sensitive Information in URL ](https://www.zaproxy.org/docs/alerts/10024/)



##### Informational (Medium)

### Description

The request appeared to contain sensitive information leaked in the URL. This can violate PCI and most organizational compliance policies. You can configure the list of strings for this check to add or remove values specific to your environment.

* URL: https://127.0.0.1:9444/api/v1/events/stream%3Fticket=846f26bc3e434c82900515420e840d9102b525d4f59e42e3b10820a1952e4f22&topics=overview
  * Node Name: `https://127.0.0.1:9444/api/v1/events/stream (ticket,topics)`
  * Method: `GET`
  * Parameter: `ticket`
  * Attack: ``
  * Evidence: `ticket`
  * Other Info: `The URL contains potentially sensitive information. The following string was found via the pattern: ticket
ticket`


Instances: 1

### Solution

Do not pass sensitive information in URIs.

### Reference



#### CWE Id: [ 598 ](https://cwe.mitre.org/data/definitions/598.html)


#### WASC Id: 13

#### Source ID: 3

### [ Modern Web Application ](https://www.zaproxy.org/docs/alerts/10109/)



##### Informational (Medium)

### Description

The application appears to be a modern web application. If you need to explore it automatically then the Client Spider may well be more effective than the standard one.

* URL: https://127.0.0.1:9444
  * Node Name: `https://127.0.0.1:9444`
  * Method: `GET`
  * Parameter: ``
  * Attack: ``
  * Evidence: `<script type="module" crossorigin src="./assets/index-Da4f1ant.js"></script>`
  * Other Info: `No links have been found while there are scripts, which is an indication that this is a modern web application.`
* URL: https://127.0.0.1:9444/
  * Node Name: `https://127.0.0.1:9444/`
  * Method: `GET`
  * Parameter: ``
  * Attack: ``
  * Evidence: `<script type="module" crossorigin src="./assets/index-Da4f1ant.js"></script>`
  * Other Info: `No links have been found while there are scripts, which is an indication that this is a modern web application.`


Instances: 2

### Solution

This is an informational alert and so no changes are required.

### Reference




#### Source ID: 3

### [ Re-examine Cache-control Directives ](https://www.zaproxy.org/docs/alerts/10015/)



##### Informational (Low)

### Description

The cache-control header has not been set properly or is missing, allowing the browser and proxies to cache content. For static assets like css, js, or image files this might be intended, however, the resources should be reviewed to ensure that no sensitive content will be cached.

* URL: https://127.0.0.1:9444
  * Node Name: `https://127.0.0.1:9444`
  * Method: `GET`
  * Parameter: `cache-control`
  * Attack: ``
  * Evidence: ``
  * Other Info: ``
* URL: https://127.0.0.1:9444/
  * Node Name: `https://127.0.0.1:9444/`
  * Method: `GET`
  * Parameter: `cache-control`
  * Attack: ``
  * Evidence: ``
  * Other Info: ``
* URL: https://127.0.0.1:9444/api/v1/overview/buckets
  * Node Name: `https://127.0.0.1:9444/api/v1/overview/buckets`
  * Method: `GET`
  * Parameter: `cache-control`
  * Attack: ``
  * Evidence: ``
  * Other Info: ``
* URL: https://127.0.0.1:9444/api/v1/session
  * Node Name: `https://127.0.0.1:9444/api/v1/session`
  * Method: `GET`
  * Parameter: `cache-control`
  * Attack: ``
  * Evidence: ``
  * Other Info: ``
* URL: https://127.0.0.1:9444/api/v1/system/endpoints
  * Node Name: `https://127.0.0.1:9444/api/v1/system/endpoints`
  * Method: `GET`
  * Parameter: `cache-control`
  * Attack: ``
  * Evidence: ``
  * Other Info: ``

Instances: Systemic


### Solution

For secure content, ensure the cache-control HTTP header is set with "no-cache, no-store, must-revalidate". If an asset should be cached consider setting the directives "public, max-age, immutable".

### Reference


* [ https://cheatsheetseries.owasp.org/cheatsheets/Session_Management_Cheat_Sheet.html#web-content-caching ](https://cheatsheetseries.owasp.org/cheatsheets/Session_Management_Cheat_Sheet.html#web-content-caching)
* [ https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Cache-Control ](https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Cache-Control)
* [ https://grayduck.mn/2021/09/13/cache-control-recommendations/ ](https://grayduck.mn/2021/09/13/cache-control-recommendations/)


#### CWE Id: [ 525 ](https://cwe.mitre.org/data/definitions/525.html)


#### WASC Id: 13

#### Source ID: 3

### [ Storable and Cacheable Content ](https://www.zaproxy.org/docs/alerts/10049/)



##### Informational (Medium)

### Description

The response contents are storable by caching components such as proxy servers, and may be retrieved directly from the cache, rather than from the origin server by the caching servers, in response to similar requests from other users. If the response data is sensitive, personal or user-specific, this may result in sensitive information being leaked. In some cases, this may even result in a user gaining complete control of the session of another user, depending on the configuration of the caching components in use in their environment. This is primarily an issue where "shared" caching servers such as "proxy" caches are configured on the local network. This configuration is typically found in corporate or educational environments, for instance.

* URL: https://127.0.0.1:9444
  * Node Name: `https://127.0.0.1:9444`
  * Method: `GET`
  * Parameter: ``
  * Attack: ``
  * Evidence: ``
  * Other Info: `In the absence of an explicitly specified caching lifetime directive in the response, a liberal lifetime heuristic of 1 year was assumed. This is permitted by rfc7234.`
* URL: https://127.0.0.1:9444/
  * Node Name: `https://127.0.0.1:9444/`
  * Method: `GET`
  * Parameter: ``
  * Attack: ``
  * Evidence: ``
  * Other Info: `In the absence of an explicitly specified caching lifetime directive in the response, a liberal lifetime heuristic of 1 year was assumed. This is permitted by rfc7234.`
* URL: https://127.0.0.1:9444/favicon.svg
  * Node Name: `https://127.0.0.1:9444/favicon.svg`
  * Method: `GET`
  * Parameter: ``
  * Attack: ``
  * Evidence: ``
  * Other Info: `In the absence of an explicitly specified caching lifetime directive in the response, a liberal lifetime heuristic of 1 year was assumed. This is permitted by rfc7234.`
* URL: https://127.0.0.1:9444/robots.txt
  * Node Name: `https://127.0.0.1:9444/robots.txt`
  * Method: `GET`
  * Parameter: ``
  * Attack: ``
  * Evidence: ``
  * Other Info: `In the absence of an explicitly specified caching lifetime directive in the response, a liberal lifetime heuristic of 1 year was assumed. This is permitted by rfc7234.`
* URL: https://127.0.0.1:9444/sitemap.xml
  * Node Name: `https://127.0.0.1:9444/sitemap.xml`
  * Method: `GET`
  * Parameter: ``
  * Attack: ``
  * Evidence: ``
  * Other Info: `In the absence of an explicitly specified caching lifetime directive in the response, a liberal lifetime heuristic of 1 year was assumed. This is permitted by rfc7234.`

Instances: Systemic


### Solution

Validate that the response does not contain sensitive, personal or user-specific information. If it does, consider the use of the following HTTP response headers, to limit, or prevent the content being stored and retrieved from the cache by another user:
Cache-Control: no-cache, no-store, must-revalidate, private
Pragma: no-cache
Expires: 0
This configuration directs both HTTP 1.0 and HTTP 1.1 compliant caching servers to not store the response, and to not retrieve the response (without validation) from the cache, in response to a similar request.

### Reference


* [ https://datatracker.ietf.org/doc/html/rfc7234 ](https://datatracker.ietf.org/doc/html/rfc7234)
* [ https://datatracker.ietf.org/doc/html/rfc7231 ](https://datatracker.ietf.org/doc/html/rfc7231)
* [ https://www.w3.org/Protocols/rfc2616/rfc2616-sec13.html ](https://www.w3.org/Protocols/rfc2616/rfc2616-sec13.html)


#### CWE Id: [ 524 ](https://cwe.mitre.org/data/definitions/524.html)


#### WASC Id: 13

#### Source ID: 3

### [ User Agent Fuzzer ](https://www.zaproxy.org/docs/alerts/10104/)



##### Informational (Medium)

### Description

Check for differences in response based on fuzzed User Agent (eg. mobile sites, access as a Search Engine Crawler). Compares the response statuscode and the hashcode of the response body with the original response.

* URL: https://127.0.0.1:9444/api/v1/buckets/pentest-lock/objects/presign
  * Node Name: `https://127.0.0.1:9444/api/v1/buckets/pentest-lock/objects/presign ()({key,method,expires_in_secs,query:[[]],headers:[],origin,session:{access_key_id,session_token}})`
  * Method: `POST`
  * Parameter: `Header User-Agent`
  * Attack: `Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.0)`
  * Evidence: ``
  * Other Info: ``
* URL: https://127.0.0.1:9444/api/v1/buckets/pentest-lock/objects/presign
  * Node Name: `https://127.0.0.1:9444/api/v1/buckets/pentest-lock/objects/presign ()({key,method,expires_in_secs,query:[[]],headers:[],origin,session:{access_key_id,session_token}})`
  * Method: `POST`
  * Parameter: `Header User-Agent`
  * Attack: `Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1)`
  * Evidence: ``
  * Other Info: ``
* URL: https://127.0.0.1:9444/api/v1/buckets/pentest-public/objects/presign
  * Node Name: `https://127.0.0.1:9444/api/v1/buckets/pentest-public/objects/presign ()({key,method,expires_in_secs,query:[[]],headers:[],origin,session:{access_key_id,session_token}})`
  * Method: `POST`
  * Parameter: `Header User-Agent`
  * Attack: `Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1)`
  * Evidence: ``
  * Other Info: ``
* URL: https://127.0.0.1:9444/api/v1/buckets/pentest-versioned/objects/presign
  * Node Name: `https://127.0.0.1:9444/api/v1/buckets/pentest-versioned/objects/presign ()({key,method,expires_in_secs,query:[[]],headers:[],origin,session:{access_key_id,session_token}})`
  * Method: `POST`
  * Parameter: `Header User-Agent`
  * Attack: `Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1)`
  * Evidence: ``
  * Other Info: ``
* URL: https://127.0.0.1:9444/api/v1/events/ticket
  * Node Name: `https://127.0.0.1:9444/api/v1/events/ticket`
  * Method: `POST`
  * Parameter: `Header User-Agent`
  * Attack: `Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1)`
  * Evidence: ``
  * Other Info: ``

Instances: Systemic


### Solution



### Reference


* [ https://owasp.org/www-project-web-security-testing-guide/ ](https://owasp.org/www-project-web-security-testing-guide/)



#### Source ID: 1


