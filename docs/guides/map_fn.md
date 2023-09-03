## Mapping Functions

Some tasks require you to specify a mapping function,
specifically one of the two kinds:

### ::: ralsei.map_fn.protocols.OneToOne
### ::: ralsei.map_fn.protocols.OneToMany

## Function Builders

However, if you want to reuse a function across multiple tasks,
where the function signature doesn't correspond _exactly_ to the columns,
you'll need to make a wrapper function:

```py
def download(url: str):
    return { "html": requests.get(url) }

def wrap_download(id: int, url: str):
    return { "id": id, **download(url) }

MapToNewColumns(
    table=TABLE_pages,
    select="SELECT id, html FROM {{table}}",
    columns=[ValueColumn("html", "TEXT")],
    id_fields=[IdColumn("id")], # (1)!
    fn=wrap_download,
)
```

1. This task generates an UPDATE statement like
   ```sql
   UPDATE pages
   SET html = %(html)s
   WHERE id = %(id)s;
   ```
   therefore `id` value needs to be present in the output

To make wrapping functions less cumbersome, **function builders** were introduced:

- [FnBuilder][ralsei.FnBuilder] for `OneToOne`
- and [GeneratorBuilder][ralsei.GeneratorBuilder] for `OneToMany`.

The above code block could be replaced with:

```py
def download(url: str):
    return { "html": requests.get(url) }

MapToNewColumns(
    table=TABLE_pages,
    select="SELECT id, html FROM {{table}}",
    columns=[ValueColumn("html", "TEXT")],
    id_fields=[IdColumn("id")],
    fn=FnBuilder(download).pop_id_fields("id").build(),
)
```

### `id_fields` magic

Notice how there's redundant information here.

Most of the time the columns popped **will be** the identifier columns.

```py hl_lines="5"
MapToNewColumns(
    table=TABLE_pages,
    select="SELECT id, html FROM {{table}}",
    columns=[ValueColumn("html", "TEXT")],
    id_fields=[IdColumn("id")], # redundant
    fn=FnBuilder(download).pop_id_fields("id").build(),
)
```

For this reason the builder object keeps a list of all popped field names.

If you omit the `build()` call and pass the builder object itself,
the task will be able to infer `id_fields` from its metadata.

```py hl_lines="5"
MapToNewColumns(
    table=TABLE_pages,
    select="SELECT id, html FROM {{table}}",
    columns=[ValueColumn("html", "TEXT")],
    fn=FnBuilder(download).pop_id_fields("id"),
)
```

### Function Conversion

In addition, the builders have alternative constructors for converting
between regular and generator functions:

| ------->To<br>From | OneToOne                                                                                                      | OneToMany                                                   |
|--------------------|---------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------|
| OneToOne           | [FnBuilder][ralsei.FnBuilder]                                                                                 | [GeneratorBuilder.from_fn][ralsei.GeneratorBuilder.from_fn] |
| OneToMany          | [FnBuilder.from_generator][ralsei.FnBuilder.from_generator]<br>(fails if generator yields more than one item) | [GeneratorBuilder][ralsei.GeneratorBuilder]                 |

### Wrapper order

Keep in mind that the wrappers are applied in the order they are added,
meaning that

```py
FnBuilder(download)
.pop_id_fields("id")
.rename_output({"id": "source_id"})
```

is roughly equivalent to `wrap2`

```py
def wrap1(id: int, url: str):
    output = download(url)
    return { "id": id, **output }

def wrap2(id: int, url: str):
    output = wrap1(id, url)
    return {
        ("source_id" if key == "id" else key): value
        for key, value in output.items()
    }
```

The opposite order would **fail**, since `id`
would have already been renamed to `source_id`.

### Reference

Look at `FnBuilderBase`, the base class of both builders,
to see what other methods they have

#### ::: ralsei.map_fn.builders.FnBuilderBase
