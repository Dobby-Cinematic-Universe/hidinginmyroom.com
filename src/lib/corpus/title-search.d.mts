export function searchWords(query: unknown): string[];
export function searchText(...parts: (string | null | undefined | (string | null | undefined)[])[]): string;
export function matchesAll(text: string, words: string[]): boolean;
export function matchedAlias(title: string, aliases: string[] | undefined, words: string[]): string | undefined;
