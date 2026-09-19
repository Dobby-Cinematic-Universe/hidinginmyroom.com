import assert from 'node:assert/strict';
import test from 'node:test';

import {
  frontmatterTitle,
  makeInventory,
  sourceCitations,
} from '../build-wiki-graph-seed-inventory.mjs';

test('parses paired and self-closing citation tags without crossing boundaries', () => {
  const page = `---
title: Example page
---

<SourceCitation
  sourceId="paired-source"
  claimId="CLAIM-1"
  locator="The word checked appears only inside this quoted value"
  reviewState="source-matched"
>
  Child content is irrelevant to the opening tag.
</SourceCitation>

<SourceCitation
  sourceId="self-closing-source"
  href="https://example.invalid/video"
  checked={true}
/>
`;

  assert.equal(frontmatterTitle(page, 'example.mdx'), 'Example page');
  assert.deepEqual(sourceCitations(page, 'example.mdx'), [
    {
      ordinal: 1,
      source_id: 'paired-source',
      claim_id: 'CLAIM-1',
      href: null,
      review_state: 'source-matched',
      checked_attribute_present: false,
    },
    {
      ordinal: 2,
      source_id: 'self-closing-source',
      claim_id: null,
      href: 'https://example.invalid/video',
      review_state: null,
      checked_attribute_present: true,
    },
  ]);
});

test('fails closed on a duplicate structural attribute', () => {
  assert.throws(
    () =>
      sourceCitations(
        '<SourceCitation sourceId="one" sourceId="two" />',
        'duplicate.mdx',
      ),
    /repeats sourceId/,
  );
});

test('inventory identity is deterministic and carries no authority', () => {
  const pages = [
    {
      domain: 'events',
      slug: 'example',
      title: 'Example',
      source_path: 'src/content/docs/wiki/events/example.mdx',
      source_sha256: 'a'.repeat(64),
      source_citations: [
        {
          ordinal: 1,
          source_id: 'source-one',
          claim_id: 'CLAIM-1',
          href: null,
          review_state: 'source-matched',
          checked_attribute_present: false,
        },
      ],
      claim_ids: ['CLAIM-1'],
    },
  ];
  const first = makeInventory(pages, 'b'.repeat(64));
  const second = makeInventory(pages, 'b'.repeat(64));

  assert.deepEqual(first, second);
  assert.match(first.inventory_id, /^wgsi_[0-9a-f]{32}$/);
  assert.deepEqual(first.authority, {
    semantic_mapping: false,
    identity_assertion: false,
    event_truth_assertion: false,
    catalog_import: false,
    publication: false,
  });
  assert.deepEqual(first.counts, {
    pages: 1,
    character_pages: 0,
    event_pages: 1,
    source_citations: 1,
    unique_claim_ids: 1,
    citations_without_claim_id: 0,
  });
});
