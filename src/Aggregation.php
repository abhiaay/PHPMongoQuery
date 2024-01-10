<?php

namespace Abhiaay\PhpMongoQuery;

use InvalidArgumentException;

class Aggregation
{

    public static $instance;
    protected $query;
    protected $pipeline;
    protected $match;
    protected $sort;
    protected $columns;
    protected $collection;
    protected $foreign_key;
    protected $primary_key;
    protected $lookup_limit = 1;
    protected $lookup_select;
    protected $lookup_local_field;
    protected $lookup_foreign_field;
    protected $lookup_as = null;
    protected $group = [];
    protected $lookups;
    protected $skip = 0;
    protected $count;
    protected $limit;
    public $operators = [
        '$eq', '$lt', '$gt', '$lte', '$gte', '$neq', '$exists', '$ne'
    ];

    // This intercepts instance calls ($testObj->whatever()) and handles them
    public function __call($name, $args)
    {
        return call_user_func_array(array($this, $name), $args);
    }

    // This intercepts instance calls ($testObj->whatever()) and handles them
    // The use of self::getInstance() lets us force static methods to act like instance methods
    public static function __callStatic($name, $args)
    {
        return call_user_func_array(array(self::getInstance(), $name), $args);
    }

    public static function getInstance()
    {
        return self::$instance ?: new self;
    }


    protected function get()
    {
        $query = '';

        if ($this->collection) {
            $this->doLookup();

            $query = $this->lookups[0];
        } elseif ($this->match) {
            $query = ['$match' => $this->match];
        } elseif ($this->sort) {
            $query = ['$sort' => $this->sort];
        } elseif ($this->columns) {
            $query = ['$project' => $this->columns];
        } elseif (count($this->group) >= 1) {
            $query = ['$group' => $this->group];
        } elseif ($this->count) {
            $query = ['$count' => $this->count];
        } elseif ($this->limit) {
            $query = ['$limit' => $this->limit];
        } elseif ($this->skip || $this->skip == 0) {
            $query = ['$skip' => $this->skip];
        }
        return $query;
    }

    protected function query()
    {
        $this->query = true;
        $this->doQuery();
        return $this->pipeline;
    }

    protected function where($column, $operator = null, $value = null)
    {
        // Here we will make some assumptions about the operator. If only 2 values are
        // passed to the method, we will assume that the operator is an equals sign
        // and keep going. Otherwise, we'll require the operator to be passed in.
        [$value, $operator] = $this->prepareValueAndOperator(
            $value,
            $operator,
            func_num_args() === 2
        );

        if (is_array($value)) {
            if ($operator == '$in' || $operator == '$nin') {
                $this->match[$column] = [$operator => $value];
            } else {
                $this->match[$column] = $value;
            }
        } else {
            $this->match[$column] = [$operator => $value];
        }

        return $this;
    }

    protected function whereIn($column, $value = [])
    {
        $this->where($column, '$in', $value);

        return $this;
    }

    protected function whereNotIn($column, $value = [])
    {
        $this->where($column, '$nin', $value);

        return $this;
    }

    protected function whereNull($column, $operator = '$eq')
    {
        $this->match[$column] = [$operator => null];

        return $this;
    }

    protected function count($column)
    {
        $this->count = $column;

        return $this;
    }

    protected function orWhere($value = [])
    {
        $this->match['$or'] = $value;

        return $this;
    }

    protected function whereBetween($column, $value = [])
    {
        $this->match[$column] = ['$gte' => $value[0], '$lte' => $value[1]];

        return $this;
    }

    protected function groupBy($column, $operator = null, $cond = null, $counter_true = 1, $counter_false = 0)
    {
        if (is_array($column)) {
            $this->group = $column;
            return $this;
        } else if (is_null($operator)) {
            $this->group[$column] = $operator;
        }

        if ($operator == '$count' && is_null($cond)) {
            $this->group[$column] = ['$sum' => $counter_true];
        }

        if (!is_null($operator) && !is_null($cond)) {
            $this->group[$column] = [$operator => [
                '$cond' => [
                    $cond, $counter_true, $counter_false
                ]
            ]];
        }

        return $this;
    }

    protected function orderBy($sort_by, $sort = 'asc')
    {
        if ($sort == 'asc') {
            $sort = 1;
        } else {
            $sort = -1;
        }
        $this->sort[$sort_by] = $sort;
        return $this;
    }

    protected function select($columns)
    {
        $fields = [];
        $columns = is_array($columns) ? $columns : func_get_args();
        foreach ($columns as $as => $column) {
            if (is_string($as)) {
                $fields[$as] = $column;
            } else {
                $fields[$column] = 1;
            }
        }

        if ($this->lookup_select) {
            $this->lookup_select = $fields;
        } else {
            $this->columns = $fields;
        }
        return $this;
    }

    protected function skip($skip)
    {
        // $skip = ['$skip' => $skip];
        $this->skip = $skip;
        return $this;
    }

    protected function limit($limit)
    {
        // $limit = ['$limit' => $limit];
        $this->limit = $limit;
        return $this;
    }

    protected function unwind($column, $includeArrayIndex = null, $preserveNullAndEmptyArrays = null)
    {
        $unwind = [
            'path' => "$$column",
        ];

        if (!is_null($includeArrayIndex)) {
            $unwind['includeArrayIndex'] = $includeArrayIndex;
        }

        if (!is_null($preserveNullAndEmptyArrays)) {
            $unwind['preserveNullAndEmptyArrays'] = $preserveNullAndEmptyArrays;
        }
        return [
            '$unwind' => $unwind
        ];
    }

    /**
     * Prepare the value and operator for a where clause.
     *
     * @param  string  $value
     * @param  string  $operator
     * @param  bool  $useDefault
     * @return array
     *
     * @throws \InvalidArgumentException
     */
    public function prepareValueAndOperator($value, $operator, $useDefault = false)
    {
        if ($useDefault) {
            return [$operator, '$eq'];
        } elseif ($this->invalidOperatorAndValue($operator, $value)) {
            throw new InvalidArgumentException('Illegal operator and value combination.');
        }

        return [$value, $operator];
    }

    protected function doLookup()
    {
        if ($this->collection == null) {
            // throw error here
            throw new \Exception("Collection not declared", 1);
        }
        $foreign_key = $this->foreign_key ?? '$' .  $this->collection . '_id';
        $primary_key = $this->primary_key ?? '$' .  '_id';


        $pipeline = [];
        if (!$this->isHasLookupField()) {
            if (!is_null($this->match) && count($this->match) > 0) {
                $pipeline[] = [
                    '$match' => [
                        '$expr' => [
                            '$eq' => ['$$foreign_key', $primary_key]
                        ],
                        $this->match ?? null
                    ]
                ];
            } else {
                $pipeline[] = [
                    '$match' => [
                        '$expr' => [
                            '$eq' => ['$$foreign_key', $primary_key]
                        ],
                    ]
                ];
            }
        } else if ($this->match) {
            $pipeline = $this->pipeline;

            $pipeline[] = [
                '$match' => $this->match
            ];
        }
        if ($this->lookup_select) {
            $pipeline[] = [
                '$project' => $this->lookup_select
            ];
        } elseif ($this->columns) {
            $pipeline[] = [
                '$project' => $this->columns
            ];
        }

        if (count($this->group) > 1) {
            $pipeline[] = ['$group' => $this->group];
        }

        if ($this->count) {
            $pipeline[] = [
                '$count' => $this->count
            ];
        }

        if ($this->sort) {
            $pipeline[] = ['$sort' => $this->sort];
        }

        if ($this->lookup_limit && $this->lookup_limit > 0) {
            $pipeline[] = [
                '$limit' => $this->lookup_limit
            ];
        }

        if ($this->isHasLookupField()) {
            $lookup = [
                '$lookup' => [
                    'from' => $this->collection,
                    'localField' => $this->lookup_local_field,
                    'foreignField' => $this->lookup_foreign_field,
                    'pipeline' => $pipeline,
                    'as' => $this->lookup_as ?? $this->collection
                ]
            ];
        } else {
            $lookup = [
                '$lookup' => [
                    'from' => $this->collection,
                    'let' => [
                        'foreign_key' => $foreign_key
                    ],
                    'pipeline' => $pipeline,
                    'as' => $this->lookup_as ?? $this->collection
                ]
            ];
        }

        $this->lookups[] = $lookup;

        return $this;
    }

    protected function addFields(string $newField, array $expression)
    {
        if ($this->isHasLookupField()) {
            $this->pipeline[] = [
                '$addFields' => [
                    $newField => $expression
                ]
            ];

            return $this;
        }

        return [
            '$addFields' => [
                $newField => $expression
            ]
        ];
    }

    protected function unset(string $field)
    {
        return [
            '$unset' => $field
        ];
    }

    protected function lookupAs(string $as = null)
    {
        $this->lookup_as = $as;

        return $this;
    }

    protected function doQuery()
    {
        $pipeline = [];

        // match
        if ($this->match) {
            $pipeline[] = ['$match' => $this->match];
        }
        // lookup if exists
        if ($this->collection) {
            $this->doLookup();

            if (count($this->lookups) > 1) {
                foreach ($this->lookups as $lookup) {
                    $pipeline[] = $lookup;
                }
            } else {
                $pipeline[] = $this->lookups[0];
            }
        }

        if ($this->columns) {
            $pipeline[] = ['$project' => $this->columns];
        }

        if (count($this->group) > 1) {
            $pipeline[] = ['$group' => $this->group];
        }

        if ($this->sort) {
            $pipeline[] = ['$sort' => $this->sort];
        }

        if ($this->skip || $this->skip == 0) {
            $pipeline[] = ['$skip' => $this->skip];
        }

        if ($this->limit) {
            $pipeline[] = ['$limit' => $this->limit];
        }

        $this->pipeline = $pipeline;

        return $this;
    }

    protected function lookup($collection, $foreign_key = null, $primary_key = null, $limit = 1)
    {
        if (is_array($foreign_key)) {
            $this->lookup_select = true;
            $this->select($foreign_key);
        }
        $this->foreign_key = $foreign_key;
        $this->primary_key = $primary_key;
        $this->collection = $collection;
        $this->lookup_limit = $limit;
        return $this;
    }

    protected function lookupField($localField, $foreignField)
    {
        $this->lookup_local_field = $localField;
        $this->lookup_foreign_field = $foreignField;

        return $this;
    }

    private function isHasLookupField()
    {
        return $this->lookup_local_field && $this->lookup_foreign_field;
    }

    protected function sample($numberSample = 1)
    {
        return [
            '$sample' => [
                'size' => $numberSample
            ]
        ];
    }

    /**
     * Determine if the given operator and value combination is legal.
     *
     * Prevents using Null values with invalid operators.
     *
     * @param  string  $operator
     * @param  mixed  $value
     * @return bool
     */
    protected function invalidOperatorAndValue($operator, $value)
    {
        return is_null($value);
    }
}
