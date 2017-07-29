<?php declare(strict_types = 1);

/**
 * This file is part of the Nextras\Orm library.
 * @license    MIT
 * @link       https://github.com/nextras/orm
 */

namespace Nextras\Orm\Mapper\Dbal;

use Nextras\Dbal\QueryBuilder\QueryBuilder;
use Nextras\Orm\Collection\Helpers\ConditionParserHelper;
use Nextras\Orm\Collection\ICollection;
use Nextras\Orm\Entity\Reflection\EntityMetadata;
use Nextras\Orm\Entity\Reflection\PropertyMetadata;
use Nextras\Orm\Entity\Reflection\PropertyRelationshipMetadata as Relationship;
use Nextras\Orm\InvalidArgumentException;
use Nextras\Orm\InvalidStateException;
use Nextras\Orm\Mapper\Dbal\CustomFunctions\IQueryBuilderFilterFunction;
use Nextras\Orm\Mapper\Dbal\CustomFunctions\IQueryBuilderNestedFilterFunction;
use Nextras\Orm\Mapper\Dbal\Helpers\ColumnReference;
use Nextras\Orm\Mapper\Dbal\StorageReflection\IStorageReflection;
use Nextras\Orm\Model\IModel;
use Nextras\Orm\Repository\Functions\ConjunctionOperatorFunction;
use Nextras\Orm\Repository\Functions\DisjunctionOperatorFunctions;
use Nextras\Orm\Repository\Functions\ValueOperatorFunction;
use Nextras\Orm\Repository\IRepository;


/**
 * QueryBuilder helper for Nextras\Dbal.
 */
class QueryBuilderHelper
{
	/** @var IModel */
	private $model;

	/** @var IRepository */
	private $repository;

	/** @var DbalMapper */
	private $mapper;


	public static function getAlias(string $name): string
	{
		static $counter = 1;
		if (preg_match('#^([a-z0-9_]+\.){0,2}+([a-z0-9_]+?)$#i', $name, $m)) {
			return $m[2];
		}

		return '_join' . $counter++;
	}


	public function __construct(IModel $model, IRepository $repository, DbalMapper $mapper)
	{
		$this->model = $model;
		$this->repository = $repository;
		$this->mapper = $mapper;
	}


	public function processCallExpr(QueryBuilder $builder, array $expr)
	{
		$operator = isset($expr[0]) ? array_shift($expr) : ICollection::AND;
		$customFunction = $this->getFunction($operator);
		if ($customFunction instanceof IQueryBuilderNestedFilterFunction) {
			$args = $customFunction->processQueryBuilderFilter($this, $builder, $expr);
			$builder->andWhere(...$args);

		} elseif ($customFunction instanceof IQueryBuilderFilterFunction) {
			$customFunction->processQueryBuilderFilter($this, $builder, $expr);

		} else {
			throw new InvalidStateException("Custom function $operator has to implement IQueryBuilderFilterFunction or IQueryBuilderNestedFilterFunction interface.");
		}
	}


	public function processNestedCallExpr(QueryBuilder $builder, array $expr): array
	{
		$operator = isset($expr[0]) ? array_shift($expr) : ICollection::AND;
		$customFunction = $this->getFunction($operator);
		if (!$customFunction instanceof IQueryBuilderNestedFilterFunction) {
			throw new InvalidStateException("Custom function $operator has to implement IQueryBuilderNestedFilterFunction interface.");
		}

		return $customFunction->processQueryBuilderFilter($this, $builder, $expr);
	}


	public function processPropertyExpr(QueryBuilder $builder, string $propertyExpr): ColumnReference
	{
		list($chain, $sourceEntity) = ConditionParserHelper::parsePropertyExpr($propertyExpr);
		$propertyName = array_pop($chain);
		list($storageReflection, $alias, $entityMetadata) = $this->normalizeAndAddJoins($chain, $sourceEntity, $builder);
		assert($storageReflection instanceof IStorageReflection);
		assert($entityMetadata instanceof EntityMetadata);
		$propertyMetadata = $entityMetadata->getProperty($propertyName);
		$column = $this->toColumnExpr($entityMetadata, $propertyMetadata, $storageReflection, $alias);
		return new ColumnReference($column, $propertyMetadata, $entityMetadata, $storageReflection);
	}


	/**
	 * @return array [IStorageReflection $sourceReflection, string $sourceAlias, EntityMetadata $sourceEntityMeta]
	 */
	private function normalizeAndAddJoins(array $levels, $sourceEntity, QueryBuilder $builder): array
	{
		$sourceMapper = $this->mapper;
		$sourceAlias = $builder->getFromAlias();
		$sourceReflection = $sourceMapper->getStorageReflection();
		$sourceEntityMeta = $sourceMapper->getRepository()->getEntityMetadata($sourceEntity);

		foreach ($levels as $levelIndex => $level) {
			$property = $sourceEntityMeta->getProperty($level);
			if ($property->relationship === null) {
				throw new InvalidArgumentException("Entity {$sourceEntityMeta->className}::\${$level} does not contain a relationship.");
			}

			$targetMapper = $this->model->getRepository($property->relationship->repository)->getMapper();
			assert($targetMapper instanceof DbalMapper);
			$targetReflection = $targetMapper->getStorageReflection();
			$targetEntityMetadata = $property->relationship->entityMetadata;

			$relType = $property->relationship->type;
			if ($relType === Relationship::ONE_HAS_MANY) {
				$targetColumn = $targetReflection->convertEntityToStorageKey($property->relationship->property);
				$sourceColumn = $sourceReflection->getStoragePrimaryKey()[0];
				$this->makeDistinct($builder);

			} elseif ($relType === Relationship::ONE_HAS_ONE && !$property->relationship->isMain) {
				$targetColumn = $targetReflection->convertEntityToStorageKey($property->relationship->property);
				$sourceColumn = $sourceReflection->getStoragePrimaryKey()[0];

			} elseif ($relType === Relationship::MANY_HAS_MANY) {
				$targetColumn = $targetReflection->getStoragePrimaryKey()[0];
				$sourceColumn = $sourceReflection->getStoragePrimaryKey()[0];
				$this->makeDistinct($builder);

				if ($property->relationship->isMain) {
					list($joinTable, list($inColumn, $outColumn)) = $sourceMapper->getManyHasManyParameters($property, $targetMapper);
				} else {
					$sourceProperty = $targetEntityMetadata->getProperty($property->relationship->property);
					list($joinTable, list($outColumn, $inColumn)) = $targetMapper->getManyHasManyParameters($sourceProperty, $sourceMapper);
				}

				$builder->leftJoin(
					$sourceAlias,
					$joinTable,
					self::getAlias($joinTable),
					"[$sourceAlias.$sourceColumn] = [$joinTable.$inColumn]"
				);

				$sourceAlias = $joinTable;
				$sourceColumn = $outColumn;

			} else {
				$targetColumn = $targetReflection->getStoragePrimaryKey()[0];
				$sourceColumn = $sourceReflection->convertEntityToStorageKey($level);
			}

			$targetTable = $targetMapper->getTableName();
			$targetAlias = $level . str_repeat('_', $levelIndex);

			$builder->leftJoin(
				$sourceAlias,
				$targetTable,
				$targetAlias,
				"[$sourceAlias.$sourceColumn] = [$targetAlias.$targetColumn]"
			);

			$sourceAlias = $targetAlias;
			$sourceMapper = $targetMapper;
			$sourceReflection = $targetReflection;
			$sourceEntityMeta = $targetEntityMetadata;
		}

		return [$sourceReflection, $sourceAlias, $sourceEntityMeta];
	}


	/**
	 * @return string|array
	 */
	private function toColumnExpr(EntityMetadata $entityMetadata, PropertyMetadata $propertyMetadata, IStorageReflection $storageReflection, string $alias)
	{
		if ($propertyMetadata->isPrimary && $propertyMetadata->isVirtual) { // primary-proxy
			$primaryKey = $entityMetadata->getPrimaryKey();
			if (count($primaryKey) > 1) { // composite primary key
				$pair = [];
				foreach ($primaryKey as $columnName) {
					$columnName = $storageReflection->convertEntityToStorageKey($columnName);
					$pair[] = "{$alias}.{$columnName}";
				}
				return $pair;

			} else {
				$propertyName = $primaryKey[0];
			}
		} else {
			$propertyName = $propertyMetadata->name;
		}

		$columnName = $storageReflection->convertEntityToStorageKey($propertyName);
		$columnExpr = "{$alias}.{$columnName}";
		return $columnExpr;
	}


	private function makeDistinct(QueryBuilder $builder)
	{
		$baseTable = $builder->getFromAlias();
		$primaryKey = $this->mapper->getStorageReflection()->getStoragePrimaryKey();

		$groupBy = [];
		foreach ($primaryKey as $column) {
			$groupBy[] = "[{$baseTable}.{$column}]";
		}

		$builder->groupBy(...$groupBy);
	}


	// todo: optimize
	private function getFunction(string $operator)
	{
		if ($operator === ValueOperatorFunction::class) {
			return new ValueOperatorFunction();
		} elseif ($operator === ConjunctionOperatorFunction::class) {
			return new ConjunctionOperatorFunction();
		} elseif ($operator === DisjunctionOperatorFunctions::class) {
			return new DisjunctionOperatorFunctions();
		} else {
			return $this->repository->getCustomFunction($operator);
		}
	}
}
