
# arspg

`arspg` is a simplified PostgreSQL client library for the D programming language. It extracts only the PostgreSQL-related parts from the excellent [`arsd`](https://github.com/adamdruppe/arsd) library by Adam D. Ruppe, making it more lightweight and easier to use.

## Installation

Add `arspg` to your `dub.json`:

```json
"dependencies": {
    "arspg": "~master"
}
```

Or to your `dub.sdl`:

```sdl
dependency "arspg" version="~master"
```

## Usage

```d
import arspg;

void main() {
    auto db = new PgConnection("host=localhost dbname=test user=postgres password=secret");
    auto result = db.query("SELECT id, name FROM users WHERE active = $1", true);

    foreach (row; result) {
        writeln("User: ", row["name"]);
    }
}
```

## License

This project follows the same licensing terms as `arsd`. See the original [arsd license](https://github.com/adamdruppe/arsd) for details.
