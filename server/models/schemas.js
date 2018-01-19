module.exports = {
  matches: {
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

    similarity: Number,
    created_at: Date,
    updated_at: Date,
  },

  typeahead: {
    field: String,
    type: String,
    value: String,
  },

  config: {
    infiles: String,
    metadata: Object,
    n_permutations: Number,
    hashband_length: Number,
    window_size: Number,
    max_cores: Number,
    step: Number,
    min_similarity: Number,
    xml_tag: String,
    flushall: Boolean,
  },

  metadata: {
    file_id: Number,
    filename: String,
    path: String,
    metadata: Object,
  },
}