const validMatrix = m => m && ['a','b','c','d','e','f'].every(key => Number.isFinite(m[key]));
export function screenPoint(point, matrix) {
  return { x:matrix.a*point.x+matrix.c*point.y+matrix.e, y:matrix.b*point.x+matrix.d*point.y+matrix.f };
}
function localPoint(point, matrix) {
  const determinant=matrix.a*matrix.d-matrix.b*matrix.c;
  if(!Number.isFinite(determinant)||Math.abs(determinant)<1e-12)return null;
  const x=point.x-matrix.e,y=point.y-matrix.f;
  return {x:(matrix.d*x-matrix.c*y)/determinant,y:(-matrix.b*x+matrix.a*y)/determinant};
}
/** Pick in CSS pixels, excluding both the plot clip and the visible scroll-frame client area. */
export function nearbyMapPoints(points, pointer, layerMatrix, svgMatrix, clip, tolerance=8, visibleBounds=null) {
  if(!validMatrix(layerMatrix)||!validMatrix(svgMatrix)||!Number.isFinite(pointer.x)||!Number.isFinite(pointer.y)||!Number.isFinite(tolerance)||tolerance<0)return [];
  const inside=p=>p&&p.x>=clip.x&&p.x<=clip.x+clip.width&&p.y>=clip.y&&p.y<=clip.y+clip.height;
  if(visibleBounds&&(!['x','y','width','height'].every(key=>Number.isFinite(visibleBounds[key]))||visibleBounds.width<0||visibleBounds.height<0))return [];
  const visible=p=>!visibleBounds||(p.x>=visibleBounds.x&&p.x<=visibleBounds.x+visibleBounds.width&&p.y>=visibleBounds.y&&p.y<=visibleBounds.y+visibleBounds.height);
  if(!visible(pointer)||!inside(localPoint(pointer,svgMatrix)))return [];
  const found=[];
  for(const point of points){const screen=screenPoint(point,layerMatrix);if(!visible(screen)||!inside(localPoint(screen,svgMatrix)))continue;const distance=Math.hypot(screen.x-pointer.x,screen.y-pointer.y);if(distance<=tolerance)found.push({id:point.id,distance});}
  return found.sort((a,b)=>a.distance-b.distance||a.id.localeCompare(b.id));
}
