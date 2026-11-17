export interface PassageExplorerDimension { id: string; label: string; standing: 'supported' | 'at the line' | 'raters do not agree' | 'none' }
export function mountPassageExplorer(root: HTMLElement, options: { dimensions: PassageExplorerDimension[]; loadingsSha256?: string | null; endpoint?: string }): void;
