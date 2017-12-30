module.exports = {
  clustered_matches: {
    match_id: Number,
    match_ids: [Number],

    source_file_id: Number,
    target_file_id: Number,

    source_segment_ids: [Number],
    target_segment_ids: [Number],

    source_prematch: String,
    target_prematch: String,

    source_match: String,
    target_match: String,

    source_postmatch: String,
    target_postmatch: String,

    source_filename: String,
    target_filename: String,

    source_year: String,
    target_year: String,

    source_author: String,
    target_author: String,

    source_title: String,
    target_title: String,

    source_module: String,
    target_module: String,

    similarity: Number,
    categorical_attribute: String,
    matching_segment_count: Number,

    created_at: Date,
    updated_at: Date,
  },

  config: {
    path_to_corpus: String,
    path_to_metadata: String,

    max_processes: Number,
    max_cached_records: Number,
    log_frequency: Number,

    strip_xml_tags: Number,
    xml_text_node: String,

    window_length: Number,
    step_size: Number,

    min_similarity: Number,
    permutations: Number,
    recall: Number,
    
    build_minhashes: Number,
    build_matches: Number,
  },

  matches: {
    match_id: Number,

    source_file_id: Number,
    target_file_id: Number,

    source_segment_id: Number,
    target_segment_id: Number,

    source_prematch: String,
    target_prematch: String,

    source_match: String,
    target_match: String,

    source_postmatch: String,
    target_postmatch: String,

    source_filename: String,
    target_filename: String,

    source_year: String,
    target_year: String,

    source_author: String,
    target_author: String,

    source_title: String,
    target_title: String,

    source_module: String,
    target_module: String,

    similarity: Number,
    categorical_attribute: String,

    created_at: Date,
    updated_at: Date,
  },

  metadata: {
    file_id: Number,
    filename: String,
    path: String,
    metadata: Object,
  },

  minhash_matches: {
    file_id: Number,
    match_file_id: Number,

    segment_id: Number,
    match_segment_id: Number,
  },

  segments: {
    file_id: Number,
    segments: Object,
  },

  texts: {
    file_id: Number,
    text: String,
  },

  typeahead_values: {
    field: String,
    type: String,
    value: String,
  }
}