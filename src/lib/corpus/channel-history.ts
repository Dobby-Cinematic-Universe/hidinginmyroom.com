import {readFile} from 'node:fs/promises';
import {readFileSync} from 'node:fs';
import path from 'node:path';
import {localPreviewRoot} from '../local-preview';
import {projectChannelHistory} from './public-channel-history.mjs';

export interface SubscriberReading {date:string;subscribers:number;rounded_to:number;source:string}
export interface ChannelHistory {
  channels:{key:'main'|'second';label:string;readings:SubscriberReading[]}[];
  early_views:{period:string;videos:number;median_views:number;p25:number;p75:number;median_age_days:number;per_1000_subscribers:number|null}[];
}

/** Channel history sidecar; empty when the release has none. */
export async function loadChannelHistory():Promise<ChannelHistory>{
  const root=localPreviewRoot();
  try{
    const text=root?readFileSync(path.join(root,'corpus/channel-history.json'),'utf8'):await readFile('src/data/corpus/channel-history.json','utf8');
    return projectChannelHistory(JSON.parse(text)) as ChannelHistory;
  }catch(error){if((error as NodeJS.ErrnoException).code!=='ENOENT')throw error;return {channels:[],early_views:[]};}
}
