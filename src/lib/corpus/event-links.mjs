export function eventHref(slug) {
  return /^event-[a-f0-9]{24}$/.test(slug)
    ? `/corpus/event-descriptions/${slug.slice(6,8)}/#${slug}`
    : `/corpus/events/${slug}/`;
}
