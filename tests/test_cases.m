%-----------------------------------------------------------------------------%
% vim: ft=mercury ts=4 sw=4 et
%-----------------------------------------------------------------------------%

:- module test_cases.
:- interface.

:- import_module csv.
:- import_module list.
:- import_module maybe.

%-----------------------------------------------------------------------------%

:- type test_type
    --->    test_type_valid
    ;       test_type_invalid.

:- type test_case
    --->    test_case(
                test_type   :: test_type,
                test_name   :: string,
                test_params :: maybe(reader_params),
                test_header :: header_desc,
                test_record :: record_desc
            ).

:- type test_cases == list(test_case).

%-----------------------------------------------------------------------------%

:- func tests = test_cases.

%-----------------------------------------------------------------------------%
%-----------------------------------------------------------------------------%

:- implementation.

:- import_module bool.
:- import_module int.
:- import_module maybe.
:- import_module string.
:- import_module univ.

%-----------------------------------------------------------------------------%

tests = [
    test_case(
        test_type_valid,
        "bool1",
        no,
        no_header,
        [
            field_desc(bool(tf_to_bool, []), no_limit, do_not_trim_whitespace),
            field_desc(bool(tf_to_bool, [negate_bool]), no_limit, do_not_trim_whitespace)
        ]
    ),

    % As above, but allow whitespace trimming.
    test_case(
        test_type_valid,
        "bool2",
        no,
        no_header,
        [
            field_desc(bool(tf_to_bool, []), no_limit, trim_whitespace),
            field_desc(bool(tf_to_bool, [negate_bool]), no_limit, trim_whitespace)
        ]
    ),

    % As above, but with quoted values.
    test_case(
        test_type_valid,
        "bool3",
        no,
        no_header,
        [
            field_desc(bool(tf_to_bool, []), no_limit, trim_whitespace),
            field_desc(bool(tf_to_bool, [negate_bool]), no_limit, trim_whitespace)
        ]
    ),

    % As above, but with a header.
    test_case(
        test_type_valid,
        "bool4",
        no,
        header_desc(no_limit),
        [
            field_desc(bool(tf_to_bool, []), no_limit, trim_whitespace),
            field_desc(bool(tf_to_bool, [negate_bool]), no_limit, trim_whitespace)
        ]
    ),

    test_case(
        test_type_valid,
        "regression1",
        no,
        no_header,
        [
            field_desc(string([]), limited(3), do_not_trim_whitespace),
            field_desc(string([]), limited(3), do_not_trim_whitespace),
            field_desc(string([]), limited(3), do_not_trim_whitespace),
            field_desc(string([]), limited(3), do_not_trim_whitespace),
            field_desc(string([]), limited(3), do_not_trim_whitespace),
            field_desc(string([]), limited(3), do_not_trim_whitespace),
            field_desc(string([]), limited(3), do_not_trim_whitespace)
        ]
    ),

    test_case(
        test_type_valid,
        "regression2",
        no,
        no_header,
        [
            field_desc(string([]), limited(1), do_not_trim_whitespace),
            field_desc(string([]), limited(2), do_not_trim_whitespace),
            field_desc(string([]), limited(4), do_not_trim_whitespace)
        ]
    ),

    test_case(
        test_type_valid,
        "regression3",
        no,
        no_header,
        [
            field_desc(string([]), limited(3), do_not_trim_whitespace),
            field_desc(string([]), limited(3), do_not_trim_whitespace),
            field_desc(string([]), limited(3), do_not_trim_whitespace),
            field_desc(string([]), limited(3), do_not_trim_whitespace)
        ]
    ),

    test_case(
        test_type_valid,
        "regression4",
        no,
        no_header,
        [
            field_desc(string([]), no_limit, do_not_trim_whitespace),
            field_desc(string([]), no_limit, do_not_trim_whitespace),
            field_desc(string([]), no_limit, do_not_trim_whitespace)
        ]
    ),

    test_case(
        test_type_invalid,
        "regression4-bad",
        no,
        no_header,
        [
            field_desc(string([]), no_limit, do_not_trim_whitespace),
            field_desc(string([]), limited(2), do_not_trim_whitespace),
            field_desc(string([]), limited(2), do_not_trim_whitespace)
        ]
    ),

    test_case(
        test_type_valid,
        "real1",
        no,
        header_desc(limited(255)),
        [
           field_desc(string([]), limited(100), trim_whitespace),
           field_desc(float([]), limited(30), trim_whitespace),
           field_desc(float([]), limited(30), trim_whitespace)
        ]
    ),

    test_case(
        test_type_valid,
        "real2",
        no,
        header_desc(limited(255)),
        [
            field_desc(string([]), limited(200), trim_whitespace),
            field_desc(string([]), limited(50), trim_whitespace),
            field_desc(string([]), limited(60), trim_whitespace),
            field_desc(string([]), limited(1000), trim_whitespace),
            field_desc(
                int(do_not_allow_floats, [require_non_negative]),
                limited(255),
                trim_whitespace
            ),
            field_desc(date(dd_mm_yyyy("/"), []), limited(255), trim_whitespace),
            field_desc(date(dd_mm_yyyy("/"), []), limited(255), trim_whitespace),
            field_desc(
                string([require_task_type]),
                limited(30),
                trim_whitespace
            )
        ]
    ),

    test_case(
        test_type_valid,
        "wiki1",
        no,
        no_header,
        [
            field_desc(int(do_not_allow_floats, []), limited(4), do_not_trim_whitespace),
            field_desc(string([]), limited(100), do_not_trim_whitespace),
            field_desc(string([]), limited(4), do_not_trim_whitespace),
            field_desc(string([]), no_limit, do_not_trim_whitespace)
        ]
    ),

    test_case(
        test_type_valid,
        "wiki2",
        no,
        header_desc(limited(100)),
        [
            field_desc(int(do_not_allow_floats, []), limited(4), do_not_trim_whitespace),
            field_desc(string([]), limited(255), do_not_trim_whitespace),
            field_desc(string([]), limited(255), do_not_trim_whitespace),
            field_desc(string([]), limited(500), do_not_trim_whitespace),
            field_desc(float([]), limited(8), do_not_trim_whitespace)
        ]
    ),

    test_case(
        test_type_valid,
        "date1",
        no,
        no_header,
        [
            field_desc(date(yyyy_mm_dd("-"), []), limited(10), do_not_trim_whitespace),
            field_desc(date(dd_mm_yyyy("."), []), limited(10), do_not_trim_whitespace),
            field_desc(date(mm_dd_yyyy(","), []), limited(10), do_not_trim_whitespace)
        ]
    ),

    test_case(
        test_type_valid,
        "date2",
        no,
        no_header,
        [
            field_desc(date(yyyy_b_dd("-"), []), limited(14), do_not_trim_whitespace),
            field_desc(date(b_dd_yyyy("-"), []), limited(14), do_not_trim_whitespace),
            field_desc(date(dd_b_yyyy("-"), []), limited(14), do_not_trim_whitespace)
        ]
    ),

    test_case(
        test_type_valid,
        "countrylist",
        no,
        header_desc(limited(50)),
        [
            field_desc(int(do_not_allow_floats, []), limited(3), do_not_trim_whitespace),
            field_desc(string([]), limited(200), do_not_trim_whitespace),
            field_desc(string([]), limited(200), do_not_trim_whitespace),
            field_desc(string([]), no_limit, do_not_trim_whitespace),
            field_desc(string([]), no_limit, do_not_trim_whitespace),
            field_desc(string([]), no_limit, do_not_trim_whitespace),
            field_desc(string([]), no_limit, do_not_trim_whitespace),
            field_desc(string([]), no_limit, do_not_trim_whitespace),
            field_desc(string([]), no_limit, do_not_trim_whitespace),
            field_desc(string([]), no_limit, do_not_trim_whitespace),
            field_desc(string([]), no_limit, do_not_trim_whitespace),
            field_desc(string([]), no_limit, do_not_trim_whitespace),
            field_desc(string([]), no_limit, do_not_trim_whitespace),
            field_desc(string([]), no_limit, do_not_trim_whitespace)
        ]
    ),

    test_case(
        test_type_valid,
        "companies",
        no,
        header_desc(limited(50)),
        [
            field_desc(string([]), limited(200), do_not_trim_whitespace),
            field_desc(string([]), limited(4), do_not_trim_whitespace),
            field_desc(string([require_industry_group]), limited(200), do_not_trim_whitespace)
        ]
    ),

    % Check if a date field contains an invalid date, e.g. Feb. 30.
    %
    test_case(
        test_type_invalid,
        "invalid_date1",
        no,
        no_header,
        [
            field_desc(date(dd_mm_yyyy("/"), []), no_limit, do_not_trim_whitespace),
            field_desc(date(dd_mm_yyyy("/"), []), no_limit, do_not_trim_whitespace)
        ]
    ),

    % Test if there are too many fields in a record.
    %
    test_case(
        test_type_invalid,
        "field_limit",
        no,
        no_header,
        [
            field_desc(string([]), no_limit, do_not_trim_whitespace),
            field_desc(string([]), no_limit, do_not_trim_whitespace),
            field_desc(string([]), no_limit, do_not_trim_whitespace)
        ]
    ),

    test_case(
        test_type_invalid,
        "header_eof",
        no,
        header_desc(limited(255)),
        [
            field_desc(string([]), no_limit, do_not_trim_whitespace),
            field_desc(string([]), no_limit, do_not_trim_whitespace),
            field_desc(string([]), no_limit, do_not_trim_whitespace)
        ]
    ),

    test_case(
        test_type_invalid,
        "unmatched_quote",
        no,
        no_header,
        [
            field_desc(string([]), no_limit, do_not_trim_whitespace),
            field_desc(string([]), no_limit, do_not_trim_whitespace)
        ]
    ),

    test_case(
        test_type_invalid,
        "unmatched_quote2",
        no,
        no_header,
        [
            field_desc(string([]), no_limit, do_not_trim_whitespace),
            field_desc(string([]), no_limit, do_not_trim_whitespace)
        ]
    ),

    test_case(
        test_type_invalid,
        "unmatched_quote3",
        no,
        no_header,
        [
            field_desc(string([]), no_limit, do_not_trim_whitespace),
            field_desc(string([]), no_limit, do_not_trim_whitespace)
        ]
    ),

    test_case(
        test_type_invalid,
        "width_limit",
        no,
        no_header,
        [
            field_desc(string([]), limited(3), do_not_trim_whitespace),
            field_desc(string([]), limited(3), do_not_trim_whitespace)
        ]
    ),

    test_case(
        test_type_invalid,
        "width_limit2",
        no,
        no_header,
        [
            field_desc(string([]), limited(6), do_not_trim_whitespace),
            field_desc(string([]), limited(3), do_not_trim_whitespace)
        ]
    ),

    test_case(
        test_type_valid,
        "univ",
        no,
        no_header,
        [
            field_desc(univ(fruit_to_univ, []), limited(6), do_not_trim_whitespace),
            field_desc(univ(fruit_to_univ, []), limited(6), do_not_trim_whitespace),
            field_desc(univ(fruit_to_univ, []), limited(6), do_not_trim_whitespace)
        ]
    ),

    test_case(
        test_type_valid,
        "discard",
        no,
        no_header,
        [
            field_desc(string([]), limited(6), do_not_trim_whitespace),
            discard(no_limit),
            field_desc(int(do_not_allow_floats, []), no_limit, do_not_trim_whitespace)
        ]
    ),

    test_case(
        test_type_invalid,
        "discard-bad",
        no,
        no_header,
        [
            field_desc(string([]), limited(6), do_not_trim_whitespace),
            discard(limited(3)),
            field_desc(string([]), limited(6), do_not_trim_whitespace)
        ]
    ),

    test_case(
        test_type_valid,
        "date_time",
        no,
        no_header,
        [
            field_desc(date_time(mm_dd_yyyy_hh_mm("-", " ", ":"), []), no_limit, do_not_trim_whitespace),
            field_desc(date_time(mm_dd_yyyy_hh_mm("-", " ", ":"), []), no_limit, do_not_trim_whitespace)
        ]
    ),

    test_case(
        test_type_valid,
        "date_time2",
        no,
        no_header,
        [
            field_desc(date_time(dd_mm_yyyy_hh_mm("/", " ", ":"), []), no_limit, do_not_trim_whitespace),
            field_desc(int(do_not_allow_floats, []), no_limit, do_not_trim_whitespace)
        ]
    ),

    test_case(
        test_type_valid,
        "maybe",
        no,
        no_header,
        [
            field_desc(maybe(int(do_not_allow_floats, [])), no_limit, do_not_trim_whitespace),
            field_desc(maybe(int(do_not_allow_floats, [])), no_limit, do_not_trim_whitespace)
        ]
    ),

    test_case(
        test_type_invalid,
        "field_desc_mismatch",
        no,
        no_header,
        [
            field_desc(int(do_not_allow_floats, []), no_limit, do_not_trim_whitespace),
            field_desc(int(do_not_allow_floats, []), no_limit, do_not_trim_whitespace)
        ]
    ),

    test_case(
        test_type_valid,
        "conv_funcs_valid",
        no,
        no_header,
        [
            int_field_desc,
            date_field_desc(yyyy_mm_dd("-")),
            float_field_desc,
            floatstr_field_desc,
            string_field_desc,
            maybe_date_field_desc(yyyy_mm_dd("-")),
            maybe_float_field_desc,
            maybe_floatstr_field_desc,
            maybe_int_field_desc,
            maybe_string_field_desc
        ]
    ),

    test_case(
        test_type_valid,
        "blank_lines",
        yes(reader_params(ignore_blank_lines, no_trailing_fields, no_comments, ',')),
        header_desc(no_limit),
        [
            string_field_desc,
            string_field_desc,
            string_field_desc
        ]
    ),

    test_case(
        test_type_valid,
        "trailing_fields",
        yes(reader_params(no_blank_lines, ignore_trailing_fields, no_comments, ',')),
        header_desc(no_limit),
        [
            field_desc(string([]), no_limit, do_not_trim_whitespace),
            field_desc(string([]), no_limit, do_not_trim_whitespace)
        ]
    ),

    test_case(
        test_type_valid,
        "comments",
        yes(reader_params(no_blank_lines, no_trailing_fields, allow_comments('#'), ',')),
        header_desc(no_limit),
        [
            field_desc(string([]), no_limit, do_not_trim_whitespace),
            field_desc(string([]), no_limit, do_not_trim_whitespace),
            field_desc(string([]), no_limit, do_not_trim_whitespace)
        ]
    )
].

%-----------------------------------------------------------------------------%

% For use by the real2 test.

:- func require_non_negative(int) = field_action_result(int).

require_non_negative(I) =
    ( if I < 0 then error("negative frequency") else ok(I)).

:- func require_task_type(string) = field_action_result(string).

require_task_type(S) =
    ( if is_task_lov(S) then ok(S) else error("invalid type: " ++ S) ).

:- pred is_task_lov(string::in) is semidet.

is_task_lov("O&M").
is_task_lov("SO&M").

%-----------------------------------------------------------------------------%

:- func require_industry_group(string) = field_action_result(string).

require_industry_group(S) =
    ( if is_industry_group(S) then
        ok(S)
    else
        error("not an industry group: " ++ S)
    ).

:- pred is_industry_group(string::in) is semidet.

is_industry_group("Energy").
is_industry_group("Materials").
is_industry_group("Capital Goods").
is_industry_group("Commercial & Professional Services").
is_industry_group("Transportation").
is_industry_group("Automobile & Components").
is_industry_group("Consumer Durables & Apparel").
is_industry_group("Consumer Services").
is_industry_group("Media").
is_industry_group("Retailing").
is_industry_group("Food & Staples Retailing").
is_industry_group("Food Beverage & Tobacco").
is_industry_group("Household & Personal Products").
is_industry_group("Health Care Equipment & Services").
is_industry_group("Pharmaceuticals, Biotechnology & Life Sciences").
is_industry_group("Banks").
is_industry_group("Diversified Financials").
is_industry_group("Insurance").
is_industry_group("Real Estate").
is_industry_group("Software & Services").
is_industry_group("Technology Hardware & Equipment").
is_industry_group("Semiconductors & Semiconductor Equipment").
is_industry_group("Telecommunication Services").
is_industry_group("Utilities").
is_industry_group("GICS Sector Code Not Applicable").
is_industry_group("Classification Pending").

%-----------------------------------------------------------------------------%

:- func tf_to_bool(string) = field_action_result(bool).

tf_to_bool(Str) = Result :-
    ( if Str = "T" then Result = ok(yes)
    else if Str = "F" then Result = ok(no)
    else Result = error("not 'T' or 'F'")
    ).

:- func negate_bool(bool) = field_action_result(bool).

negate_bool(B) = ok(bool.not(B)).

%-----------------------------------------------------------------------------%

% For the univ test case.

:- type fruit
    --->    apple
    ;       orange
    ;       lemon
    ;       pear.

:- func fruit_to_univ : univ_handler.

fruit_to_univ(String) =
    ( if string_to_fruit(String, Fruit)
    then ok(univ(Fruit))
    else error("not a fruit")
    ).

:- pred string_to_fruit(string::in, fruit::out) is semidet.

string_to_fruit("apple",  apple).
string_to_fruit("orange", orange).
string_to_fruit("lemon",  lemon).
string_to_fruit("pear",   pear).

%-----------------------------------------------------------------------------%
:- end_module test_cases.
%-----------------------------------------------------------------------------%
