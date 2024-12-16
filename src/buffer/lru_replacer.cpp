#include "buffer/lru_replacer.h"
LRUReplacer::LRUReplacer(size_t num_pages){}

LRUReplacer::~LRUReplacer() = default;

/**
 * TODO: Student Implement
 */
bool LRUReplacer::Victim(frame_id_t *frame_id) {
  if(Size() <= 0) 
		return false;

	*frame_id = list_lru.back();
	list_lru.pop_back();
	map.erase(*frame_id);
	return true;
}

/**
 * TODO: Student Implement
 */
void LRUReplacer::Pin(frame_id_t frame_id) {
	if(map.find(frame_id) != map.end())  //the frame_id is in the map, erase it
	{
		list_lru.erase(map[frame_id]);
		map.erase(frame_id);
	}	
}

/**
 * TODO: Student Implement
 */
void LRUReplacer::Unpin(frame_id_t frame_id) {
	
	if(map.find(frame_id) != map.end())
		return;
		//	list_lru.erase(map[frame_id]);  //if the page is already in the lru-list, move it to the head 
	
	list_lru.emplace_front(frame_id);
	map[frame_id] = list_lru.begin();
}

/**
 * TODO: Student Implement
 */
size_t LRUReplacer::Size() {
  return list_lru.size();
}
