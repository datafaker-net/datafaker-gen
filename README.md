# datafaker-gen

Datafaker-gen is a [datafaker](https://github.com/datafaker-net/datafaker) based command line generator.
It does not require code writing and project rebuild each time, only configuration.

## Prerequisite
A built jar is required
you can get by 
```
./mvnw clean verify
```

## An example of usage
```bash
bin/datafaker_gen -f json -n 2 -sink cli
```
which will generate 2 random records in json format and output it to the terminal.
like 
```json
[
{"lastname": "Howell", "firstname": "麻美", "phone numbers": ["(979) 786-5201", "(708) 707-4500"], "address": {"country": "Cote d'Ivoire", "city": "East Albertoburgh", "street address": "1734 Botsford Drives"}},
{"lastname": null, "firstname": "蒼空", "phone numbers": ["(513) 352-0492", "(928) 813-7762", "(419) 616-6421"], "address": {"country": "Belgium", "city": "South Willis", "street address": "100 Dulce Turnpike"}}
]
```

It supports several formats: `csv`, `xml`, `json`, `sql` (also some dialects), `xml`, `yaml`.
Some formats have properties to configure (have a look at `output.yaml`)

Also it supports another output to text file (configuration of output is defined at `output.yaml`) like 

```bash
bin/datafaker_gen -f sql -n 10 -sink textfile
```

## Configure fields to output
Ok, what if there is a need to output just 2 fields: first name and last name?
To change it we need to change `config.yaml` (or create another one) with the content
```yaml
fields:
  - name: lastname
    generators: [ Name#lastName ]
  - name: firstname
    generators: [ Name#firstName ]
```
and run same 
```bash
bin/datafaker_gen -f json -n 2 -sink cli
```
which could output something like
```json
[
  {"lastname": "Brekke", "firstname": "Douglass"},
  {"lastname": "Bernhard", "firstname": "Daniel"}
]
```
### Null rate
It could happen that sometimes some data are missed and `null` values should be used
This behaviour could be configured with `nullRate` property like
```yaml
fields:
  - name: lastname
    nullRate: 0.1
    generators: [ Name#lastName ]
  - name: firstname
    generators: [ Name#firstName ]
```
now it says that for `lastname` with probability `0.1` it will be emitted `null` value.

### Locales

By default, it is current locale however it could be changed.
First there could be set a default locale for current config, second there could be set a locale per field like
```yaml
default_locale: en-EN
fields:
  - name: lastname
    nullRate: 0.1
    generators: [ Name#lastName ]
  - name: firstname
    locale: ja-JP
    generators: [ Name#firstName ]
```
e.g.
```
bin/datafaker_gen -f json -n 2 -sink cli
```
outputs something like
```json
[
{"lastname": "Dickens", "firstname": "正三"},
{"lastname": "Leffler", "firstname": "美香"}
]
```

## Collections
Sometimes one field should contain a collection like a person could have several phone numbers.
Let's emulate this with this config
```yaml
default_locale: en-EN
fields:
  - name: lastname
    nullRate: 0.1
    generators: [ Name#lastName ]
  - name: firstname
    locale: ja-JP
    generators: [ Name#firstName ]
  - name: phone numbers
    type: array # special type for collection field
    minLength: 2 # parameter to specify min length of array
    maxLength: 5 # parameter to specify max length of array
    generators: [ PhoneNumber#phoneNumber, PhoneNumber#cellPhone ]
```

and the result for
```
bin/datafaker_gen -f json -n 2 -sink cli
```
could be 
```json
[
{"lastname": "Littel", "firstname": "辰雄", "phone numbers": ["(270) 857-3976 x5352", "(605) 253-6302 x5863"]},
{"lastname": "Lebsack", "firstname": "英樹", "phone numbers": ["(612) 956-2065", "1-708-334-9522", "(973) 979-3113", "1-484-404-4443"]}
]
```

## Struct
Also sometimes field could be complex. For example a person could have an address consisting of `country`, `city` and a `streetAddress`.
Here `struct` type could help
```yaml
default_locale: en-EN
fields:
  - name: lastname
    nullRate: 0.1
    generators: [ Name#lastName ]
  - name: firstname
    locale: ja-JP
    generators: [ Name#firstName ]
  - name: phone numbers
    type: array
    minLength: 2
    maxLength: 5
    generators: [ PhoneNumber#phoneNumber, PhoneNumber#cellPhone ]
  - name: address
    type: struct
    fields:
      - name: country
        generators: [ Address#country ]
      - name: city
        generators: [ Address#city ]
      - name: street address
        generators: [ Address#streetAddress ]
```
and for 
```
bin/datafaker_gen -f json -n 2 -sink cli
```
it gives something like
```json
[
{"lastname": "Macejkovic", "firstname": "正治", "phone numbers": ["757-317-9481", "(707) 678-8732", "(970) 270-9267", "(224) 209-6756"], "address": {"country": "Heard Island and McDonald Islands", "city": "New Chrissy", "street address": "961 Arthur Shoal"}},
{"lastname": "Altenwerth", "firstname": "陽子", "phone numbers": ["(503) 412-1163 x5984", "1-260-832-0986"], "address": {"country": "Dominican Republic", "city": "East Ed", "street address": "955 Martin Tunnel"}}
]
```