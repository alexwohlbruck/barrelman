-- Build search_lexicon: every distinct word of geo_places.ts, with the number
-- of places containing it. Search corrects misspelt query words against it
-- (src/services/search-lexicon.service.ts).
--
-- Built into a fresh table and swapped in, so searches read the old lexicon
-- until the new one is complete and indexed. Rebuilding costs one pass over
-- the named rows; run it after anything that rewrites ts in bulk. A word that
-- appears between rebuilds is still found exactly, it just isn't offered as
-- a correction until the next one.
--
-- Left out: words under four letters (search never corrects them) and words
-- with no letter in them (house numbers, refs), which are most of the long
-- tail and have no spelling to correct towards.

DROP TABLE IF EXISTS search_lexicon_next;

-- Each word is filed under itself and every one-letter deletion of it, the
-- keys a query word is looked up by (deletionKeys() in the service makes the
-- same ones). Two words share a key when one edit separates them.
CREATE TABLE search_lexicon_next AS
SELECT word, n,
       ARRAY(SELECT DISTINCT left(word, i - 1) || substr(word, i + 1)
             FROM generate_series(1, length(word)) i) || word AS keys
FROM (
  SELECT word, count(*)::int AS n
  FROM geo_places, unnest(tsvector_to_array(ts)) AS word
  WHERE ts IS NOT NULL
    AND length(word) >= 4
    AND word ~ '[[:alpha:]]'
  GROUP BY word
) words;

CREATE INDEX search_lexicon_next_keys_idx ON search_lexicon_next USING gin (keys);
ANALYZE search_lexicon_next;

BEGIN;
DROP TABLE IF EXISTS search_lexicon;
ALTER TABLE search_lexicon_next RENAME TO search_lexicon;
ALTER INDEX search_lexicon_next_keys_idx RENAME TO search_lexicon_keys_idx;
COMMIT;
