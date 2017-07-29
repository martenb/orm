<?php declare(strict_types = 1);

/**
 * This file is part of the Nextras\Orm library.
 * @license    MIT
 * @link       https://github.com/nextras/orm
 */

namespace Nextras\Orm\Collection\Helpers;

use Closure;
use DateTimeImmutable;
use DateTimeInterface;
use Nextras\Orm\Collection\ICollection;
use Nextras\Orm\Entity\IEntity;
use Nextras\Orm\Entity\Reflection\EntityMetadata;
use Nextras\Orm\Entity\Reflection\PropertyMetadata;
use Nextras\Orm\Entity\Reflection\PropertyRelationshipMetadata;
use Nextras\Orm\InvalidStateException;
use Nextras\Orm\Mapper\IMapper;
use Nextras\Orm\Mapper\Memory\CustomFunctions\IArrayFilterFunction;
use Nextras\Orm\Mapper\Memory\CustomFunctions\IArrayNestedFilterFunction;
use Nextras\Orm\Repository\Functions\ConjunctionOperatorFunction;
use Nextras\Orm\Repository\Functions\DisjunctionOperatorFunctions;
use Nextras\Orm\Repository\Functions\ValueOperatorFunction;
use Nextras\Orm\Repository\IRepository;


class ArrayCollectionHelper
{
	/** @var IRepository */
	private $repository;

	/** @var IMapper */
	private $mapper;


	public function __construct(IRepository $repository)
	{
		$this->repository = $repository;
		$this->mapper = $repository->getMapper();
	}


	public function createFilter(array $expr): Closure
	{
		$operator = isset($expr[0]) ? array_shift($expr) : ICollection::AND;
		$customFunction = $this->getFunction($operator);

		if ($customFunction instanceof IArrayNestedFilterFunction) {
			return function (array $entities) use ($customFunction, $expr) {
				/** @var IEntity[] $entities */
				return array_filter($entities, function (IEntity $entity) use ($customFunction, $expr) {
					return $customFunction->processArrayFilter($this, $entity, $expr);
				});
			};

		} elseif ($customFunction instanceof IArrayFilterFunction) {
			return function (array $entities) use ($customFunction, $expr) {
				/** @var IEntity[] $entities */
				return $customFunction->processArrayFilter($this, $entities, $expr);
			};

		} else {
			throw new InvalidStateException("Custom function $operator has to implement IQueryBuilderFilterFunction or IQueryBuilderNestedFilterFunction interface.");
		}
	}


	public function createNestedFilterCallback(array $expr)
	{
		$operator = isset($expr[0]) ? array_shift($expr) : ICollection::AND;
		$customFunction = $this->getFunction($operator);

		if (!$customFunction instanceof IArrayNestedFilterFunction) {
			throw new InvalidStateException("Custom function $operator has to implement IQueryBuilderNestedFilterFunction interface.");
		}

		return function (IEntity $entity) use ($customFunction, $expr) {
			return $customFunction->processArrayFilter($this, $entity, $expr);
		};
	}


	public function createSorter(array $conditions): Closure
	{
		$columns = [];
		foreach ($conditions as $pair) {
			list($column, $sourceEntity) = ConditionParserHelper::parsePropertyExpr($pair[0]);
			$sourceEntityMeta = $this->repository->getEntityMetadata($sourceEntity);
			$columns[] = [$column, $pair[1], $sourceEntityMeta];
		}

		return function ($a, $b) use ($columns) {
			foreach ($columns as $pair) {
				$_a = $this->getValueByTokens($a, $pair[0], $pair[2])->value;
				$_b = $this->getValueByTokens($b, $pair[0], $pair[2])->value;
				$direction = $pair[1] === ICollection::ASC ? 1 : -1;

				if ($_a === null || $_b === null) {
					if ($_a !== $_b) {
						return $direction * ($_a === null ? -1 : 1);
					}
				} elseif (is_int($_a) || is_float($_a)) {
					if ($_a < $_b) {
						return $direction * -1;
					} elseif ($_a > $_b) {
						return $direction;
					}
				} else {
					$res = strcmp((string) $_a, (string) $_b);
					if ($res < 0) {
						return $direction * -1;
					} elseif ($res > 0) {
						return $direction;
					}
				}
			}

			return 0;
		};
	}


	public function getValue(IEntity $entity, string $expr, & $targetValue = null): ValueReference
	{
		list($tokens, $sourceEntityClassName) = ConditionParserHelper::parsePropertyExpr($expr);
		$sourceEntityMeta = $this->repository->getEntityMetadata($sourceEntityClassName);
		return $this->getValueByTokens($entity, $tokens, $sourceEntityMeta);
	}


	public function normalizeValue($value, PropertyMetadata $propertyMetadata)
	{
		if ($value instanceof IEntity) {
			return $value->hasValue('id') ? $value->getValue('id') : null;

		} elseif (isset($propertyMetadata->types['datetime']) && $value !== null) {
			if (!$value instanceof DateTimeInterface) {
				$value = new DateTimeImmutable($value);
			}
			return $value->getTimestamp();
		}

		return $value;
	}


	private function getValueByTokens(IEntity $entity, array $tokens, EntityMetadata $sourceEntityMeta): ValueReference
	{
		$isFromHasManyResult = false;
		$values = [];
		$stack = [[$entity, $tokens, $sourceEntityMeta]];

		do {
			/** @var IEntity $value */
			/** @var string[] $tokens */
			/** @var EntityMetadata $entityMeta */
			list ($value, $tokens, $entityMeta) = array_shift($stack);

			do {
				$propertyName = array_shift($tokens);
				$propertyMeta = $entityMeta->getProperty($propertyName); // check if property exists
				$value = $value->hasValue($propertyName) ? $value->getValue($propertyName) : null;

				if ($propertyMeta->relationship) {
					$entityMeta = $propertyMeta->relationship->entityMetadata;

					if (
						$propertyMeta->relationship->type === PropertyRelationshipMetadata::MANY_HAS_MANY
						|| $propertyMeta->relationship->type === PropertyRelationshipMetadata::ONE_HAS_MANY
					) {
						$isFromHasManyResult = true;
						foreach ($value as $subEntity) {
							$stack[] = [$subEntity, $tokens, $entityMeta];
						}
						continue 2;
					}
				}

			} while (count($tokens) > 0 && $value !== null);

			$values[] = $this->normalizeValue($value, $propertyMeta);
		} while (!empty($stack));

		return new ValueReference($isFromHasManyResult, $isFromHasManyResult ? $values : $values[0], $propertyMeta);
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
