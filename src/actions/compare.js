import { sort } from './favorite';
import { fetchSearchResults } from './search';

export const toggleCompare = (obj) => {
  return (dispatch, getState) => {
    const _state = getState();
    const type = obj.type;
    const file_id = obj.result[ obj.type + '_file_id' ].toString();
    const segment_ids = sort(obj.result[ obj.type + '_segment_ids' ]).join('.');
    if (_state.compare.type === type &&
        _state.compare.file_id === file_id &&
        _state.compare.segment_ids === segment_ids) {
      dispatch(endCompare())
    } else {
      dispatch(startCompare({
        type: type,
        file_id: file_id.toString(),
        segment_ids: segment_ids,
      }))
    }
    dispatch(fetchSearchResults())
  }
}

export const startCompare = (obj) => ({
  type: 'START_COMPARE', obj: obj
})

export const endCompare = () => ({
  type: 'END_COMPARE'
})

export const setCompare = (obj) => ({
  type: 'SET_COMPARE', obj: obj
})