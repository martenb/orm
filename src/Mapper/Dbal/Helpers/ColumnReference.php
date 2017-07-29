<?php

namespace Nextras\Orm\Mapper\Dbal\Helpers;

use Nextras\Orm\Entity\IEntity;
use Nextras\Orm\Entity\Reflection\EntityMetadata;
use Nextras\Orm\Entity\Reflection\PropertyMetadata;
use Nextras\Orm\Mapper\Dbal\StorageReflection\IStorageReflection;


class ColumnReference
{
	/** @var string|array */
	public $column;

	/** @var PropertyMetadata */
	public $propertyMetadata;

	/** @var EntityMetadata */
	public $entityMetadata;

	/** @var IStorageReflection */
	public $storageReflection;


	public function __construct($column, PropertyMetadata $propertyMetadata, EntityMetadata $entityMetadata, IStorageReflection $storageReflection)
	{
		$this->column = $column;
		$this->propertyMetadata = $propertyMetadata;
		$this->entityMetadata = $entityMetadata;
		$this->storageReflection = $storageReflection;
	}


	public function normalizeValue($value)
	{
		if ($value instanceof \Traversable) {
			$value = iterator_to_array($value);
		} elseif ($value instanceof IEntity) {
			$value = $value->getValue('id');
		}

		$tmp = $this->storageReflection->convertEntityToStorage([$this->propertyMetadata->name => $value]);
		$convertedValue = reset($tmp);

		if ($this->propertyMetadata->isPrimary && $this->propertyMetadata->isVirtual && count($this->entityMetadata->getPrimaryKey()) > 1) {
			if (!isset($convertedValue[0][0])) {
				$convertedValue = [$convertedValue];
			}
		}

		return $convertedValue;
	}
}
